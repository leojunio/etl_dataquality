# sharepoint_graph.py
# -*- coding: utf-8 -*-
"""
Conector para baixar/listar arquivos do SharePoint Online usando Microsoft Graph.

Autenticação suportada (defina via MS_AUTH_MODE):
- "device_code" (recomendado quando há MFA)        -> requer MSAL_CLIENT_ID (ou AZURE_CLIENT_ID)
- "ropc"        (user+password, sem MFA obrigatório)-> requer MSAL_CLIENT_ID, MS_USERNAME, MS_PASSWORD
- "app"         (client credentials / app-only)     -> requer AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Origens suportadas:
(A) Link de pasta compartilhada:
    - SHAREPOINT_FOLDER_LINK
(B) Site + pasta (com acento ok), opcionalmente informando a biblioteca:
    - SHAREPOINT_SITE_URL (ex.: https://tenant.sharepoint.com/sites/ECOLEGIS)
    - SHAREPOINT_FOLDER_PATH (ex.: "doenças" ou "Documentos/doenças")
    - SHAREPOINT_LIBRARY_NAME (opcional; ex.: "Documentos" ou "Documents")

Variáveis de ambiente comuns:
- MS_AUTH_MODE=device_code|ropc|app
- MSAL_CLIENT_ID (ou AZURE_CLIENT_ID)
- MS_USERNAME / MS_PASSWORD (apenas para ropc)
- AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET (apenas para app)
- SHAREPOINT_* conforme a origem escolhida
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple
import base64
import os
import re
import time
import urllib.parse as urlparse
from dotenv import load_dotenv
import msal
import requests
load_dotenv()

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def _encode_share_url(url: str) -> str:
    """Converte um sharing link em shareId (formato 'u!<base64url>') para /shares/{shareId}."""
    b64 = base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8").rstrip("=")
    return f"u!{b64}"


class SharePointGraphUsernameConnector:
    """
    Conector Microsoft Graph para SharePoint Online.

    Exemplos de uso (site + pasta, Device Code):

        sp = SharePointGraphUsernameConnector(
            auth_mode="device_code",
            site_url="https://tenant.sharepoint.com/sites/ECOLEGIS",
            library_name="Documentos",   # opcional se a pasta estiver dentro dela
            folder_path="doenças",       # pode conter acentos
        )
        sp.download_files(Path("data/sharepoint/doencas"), patterns=["*"], extensions=(".csv", ".xlsx"))

    Exemplos de uso (app-only / client credentials):

        sp = SharePointGraphUsernameConnector(
            auth_mode="app",
            tenant_id="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            client_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
            client_secret="***************",
            site_url="https://tenant.sharepoint.com/sites/ECOLEGIS",
            library_name="Documentos",
            folder_path="doenças",
        )
        sp.download_files(Path("data/sharepoint/doencas"))

    """

    # ---------------------- Inicialização ----------------------
    def __init__(
        self,
        # Auth
        client_id: Optional[str] = None,
        auth_mode: Optional[str] = None,  # "device_code" | "ropc" | "app"
        username: Optional[str] = None,
        password: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        session: Optional[requests.Session] = None,
        scopes: Optional[List[str]] = None,
        # Origem (A)
        shared_folder_link: Optional[str] = None,
        # Origem (B)
        site_url: Optional[str] = None,
        folder_path: Optional[str] = None,
        library_name: Optional[str] = None,
    ):
        # IDs/segredos
        self.client_id = (
            client_id
            or os.getenv("SHAREPOINT_AZURE_CLIENT_ID")
            or os.getenv("MSAL_CLIENT_ID")
        )
        self.tenant_id = tenant_id or os.getenv("SHAREPOINT_AZURE_TENANT_ID")
        self.client_secret = client_secret or os.getenv("SHAREPOINT_AZURE_CLIENT_SECRET")

        # Modo de autenticação
        self.auth_mode = (auth_mode or os.getenv("SHAREPOINT_AUTH_MODE") or "device_code").lower()
        self.username = username or os.getenv("MS_USERNAME")
        self.password = password or os.getenv("MS_PASSWORD")

        if not self.client_id:
            raise ValueError("Client ID não definido (use AZURE_CLIENT_ID ou MSAL_CLIENT_ID).")

        # Scopes:
        # - app-only usa ".default"
        # - user flows usam escopos delegados
        self.scopes = scopes or (
            ["https://graph.microsoft.com/.default"]
            if self.auth_mode == "app"
            else ["Files.Read", "Files.Read.All", "Sites.Read.All", "offline_access"]
        )

        # Sessão HTTP
        self._session = session or requests.Session()
        self._token: Optional[str] = None

        # Origem de arquivos
        self.shared_folder_link = shared_folder_link or os.getenv("SHAREPOINT_FOLDER_LINK")
        self.site_url = site_url or os.getenv("SHAREPOINT_SITE_URL")
        self.folder_path = folder_path or os.getenv("SHAREPOINT_FOLDER_PATH")
        self.library_name = library_name or os.getenv("SHAREPOINT_LIBRARY_NAME")

        if not (self.shared_folder_link or self.site_url):
            raise ValueError("Informe uma origem: SHAREPOINT_FOLDER_LINK (A) ou SHAREPOINT_SITE_URL (B).")

    # ---------------------- Autenticação ----------------------
    def _acquire_token_app(self) -> str:
        """Client Credentials (app-only)."""
        if not (self.tenant_id and self.client_secret):
            raise ValueError("Modo 'app' requer AZURE_TENANT_ID e AZURE_CLIENT_SECRET definidos.")
        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" not in result:
            raise RuntimeError(f"Falha ao obter token (app-only): {result}")
        return result["access_token"]

    def _acquire_token_ropc(self) -> str:
        """Resource Owner Password Credentials (somente se permitido e sem MFA)."""
        if not (self.username and self.password):
            raise ValueError("Modo 'ropc' requer MS_USERNAME e MS_PASSWORD.")
        app = msal.PublicClientApplication(
            client_id=self.client_id,
            authority="https://login.microsoftonline.com/organizations",
        )
        scopes = [s if s.startswith("https://") else f"https://graph.microsoft.com/{s}" for s in self.scopes]
        result = app.acquire_token_by_username_password(self.username, self.password, scopes=scopes)
        if "access_token" not in result:
            raise RuntimeError(f"Falha ao obter token (ropc): {result}")
        return result["access_token"]

    def _acquire_token_device_code(self) -> str:
        """Device Code Flow (funciona com MFA)."""
        authority = (
            f"https://login.microsoftonline.com/{self.tenant_id}"
            if self.tenant_id
            else "https://login.microsoftonline.com/organizations"
        )
        app = msal.PublicClientApplication(
            client_id=self.client_id,
            authority=authority,
        )
        scopes = [s if s.startswith("https://") else f"https://graph.microsoft.com/{s}" for s in self.scopes]
        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise RuntimeError(f"Falha ao iniciar device code: {flow}")
        print(f"[Device Code] Abra {flow['verification_uri']} e digite o código: {flow['user_code']}")
        flow["expires_at"] = int(time.time()) + 30 # Finaliza em 30 segundos
        result = app.acquire_token_by_device_flow(flow)
        if "access_token" not in result:
            raise RuntimeError(f"Falha ao obter token (device code): {result}")
        return result["access_token"]

    def _get_access_token(self) -> str:
        if self._token:
            return self._token
        if self.auth_mode == "app":
            self._token = self._acquire_token_app()
        elif self.auth_mode == "ropc":
            try:
                self._token = self._acquire_token_ropc()
            except Exception as e:
                # fallback automático para device code se ropc falhar (política/MFA)
                print(f"[Auth] ROPC falhou ({e}); tentando Device Code…")
                self._token = self._acquire_token_device_code()
        else:
            self._token = self._acquire_token_device_code()
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_access_token()}"}

    # ---------------------- Helpers Graph ----------------------
    def _resolve_driveitem_from_share(self) -> dict:
        """Resolve o driveItem raiz de um sharing link de pasta."""
        share_id = _encode_share_url(self.shared_folder_link)
        url = f"{GRAPH_BASE}/shares/{share_id}/driveItem"
        r = self._session.get(url, headers=self._headers(), timeout=60)
        r.raise_for_status()
        return r.json()

    def _list_children(self, drive_id: str, item_id: str) -> List[dict]:
        """Lista filhos (arquivos/pastas) de um item."""
        url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/children"
        items: List[dict] = []
        while url:
            r = self._session.get(url, headers=self._headers(), timeout=60)
            r.raise_for_status()
            data = r.json()
            items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
        return items

    def _get_site_by_url(self, site_url: str) -> dict:
        """Resolve metadados do site (/sites/{hostname}:{path})."""
        parsed = urlparse.urlparse(site_url)
        hostname = parsed.hostname
        path = parsed.path.rstrip("/")
        url = f"{GRAPH_BASE}/sites/{hostname}:{path}"
        r = self._session.get(url, headers=self._headers(), timeout=60)
        r.raise_for_status()
        return r.json()

    def _list_site_drives(self, site_id: str) -> List[dict]:
        """Lista bibliotecas (drives) do site."""
        url = f"{GRAPH_BASE}/sites/{site_id}/drives"
        r = self._session.get(url, headers=self._headers(), timeout=60)
        r.raise_for_status()
        return r.json().get("value", [])

    def _get_drive_and_item_for_folder(
        self, site_id: str, folder_path: str, library_name: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Resolve (drive_id, item_id) da pasta alvo.
        Estratégia:
          - Se library_name for informado: tenta nessa drive (displayName/name)
          - Senão: se folder_path começa com "<biblioteca>/...", usa essa como candidata
          - Caso contrário: testa todas as drives até achar a pasta
        """
        drives = self._list_site_drives(site_id)
        norm_path = folder_path.strip("/")

        inferred_library = None
        if "/" in norm_path and not library_name:
            inferred_library, remainder = norm_path.split("/", 1)
        else:
            remainder = norm_path

        # Monta lista de drives candidatas
        if library_name:
            candidates = [
                d for d in drives
                if d.get("name") == library_name or d.get("displayName") == library_name
            ]
        elif inferred_library:
            candidates = [
                d for d in drives
                if d.get("name") == inferred_library or d.get("displayName") == inferred_library
            ]
        else:
            candidates = drives

        last_err = None
        # Tenta resolver a pasta dentro das drives candidatas
        for drv in candidates:
            drive_id = drv["id"]
            rel = remainder if (library_name or inferred_library) else norm_path
            rel = rel.strip("/")

            # Endpoint root:/<segmentos>:
            if rel:
                segments = [urlparse.quote(s, safe="") for s in rel.split("/")]
                url = f"{GRAPH_BASE}/drives/{drive_id}/root:/" + "/".join(segments)
            else:
                url = f"{GRAPH_BASE}/drives/{drive_id}/root"

            try:
                r = self._session.get(url, headers=self._headers(), timeout=60)
                r.raise_for_status()
                item = r.json()
                if "folder" in item:
                    return drive_id, item["id"]
            except Exception as e:
                last_err = e
                continue

        # Fallback: buscar por nome da pasta na raiz de cada drive
        target = norm_path.split("/")[-1]
        for drv in drives:
            try:
                children = self._list_children(drv["id"], drv["root"]["id"])
                for it in children:
                    if "folder" in it and it["name"].lower() == target.lower():
                        return drv["id"], it["id"]
            except Exception as e:
                last_err = e
                continue

        raise FileNotFoundError(f"Pasta '{folder_path}' não encontrada no site. Último erro: {last_err}")

    @staticmethod
    def _match_any(name: str, patterns: Iterable[str]) -> bool:
        """Casamento simples tipo glob (case-insensitive)."""
        for p in patterns:
            regex = "^" + re.escape(p).replace(r"\*", ".*") + "$"
            if re.match(regex, name, flags=re.IGNORECASE):
                return True
        return False

    # ---------------------- APIs Públicas ----------------------
    def list_files(self) -> List[dict]:
        """
        Lista arquivos diretamente sob a pasta informada (não percorre recursivamente).
        Retorna metadados crus do Graph (cada item contém 'name', 'id', possivelmente '@microsoft.graph.downloadUrl', etc.).
        """
        if self.shared_folder_link:
            root = self._resolve_driveitem_from_share()
            drive_id = root["parentReference"]["driveId"]
            item_id = root["id"]
            return self._list_children(drive_id, item_id)

        if not (self.site_url and self.folder_path):
            raise ValueError("Para o modo 'site+pasta', defina SHAREPOINT_SITE_URL e SHAREPOINT_FOLDER_PATH.")
        site = self._get_site_by_url(self.site_url)
        drive_id, item_id = self._get_drive_and_item_for_folder(site["id"], self.folder_path, self.library_name)
        return self._list_children(drive_id, item_id)

    def download_files(
        self,
        dest_dir: Path,
        patterns: Iterable[str] = ("*",),
        extensions: Iterable[str] = (".csv", ".txt", ".xlsx"),
        overwrite: bool = True,
        throttle_ms: int = 0,
    ) -> List[Path]:
        """
        Baixa arquivos que casem com 'patterns' e 'extensions' para 'dest_dir'.
        Retorna a lista de Paths salvos.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        items = self.list_files()
        saved: List[Path] = []

        for it in items:
            if "file" not in it:
                # ignore subpastas neste método (não-recursivo)
                continue

            name = it["name"]
            if extensions and not any(name.lower().endswith(ext) for ext in extensions):
                continue
            if patterns and not self._match_any(name, patterns):
                continue

            dl_url = it.get("@microsoft.graph.downloadUrl")
            if not dl_url:
                # Fallback: endpoint /content
                dl_url = f"{GRAPH_BASE}/drives/{it['parentReference']['driveId']}/items/{it['id']}/content"

            out = dest_dir / name
            if out.exists() and not overwrite:
                saved.append(out)
                continue

            if dl_url.startswith("https://"):
                resp = self._session.get(dl_url, stream=True, timeout=240)
            else:
                resp = self._session.get(dl_url, headers=self._headers(), stream=True, timeout=240)
            resp.raise_for_status()

            with open(out, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)

            saved.append(out)
            if throttle_ms:
                time.sleep(throttle_ms / 1000.0)

        return saved

    # ---------------------- Utilidades opcionais ----------------------
    def list_site_libraries(self) -> List[str]:
        """
        Retorna os nomes (displayName) de todas as bibliotecas do site (útil para descobrir 'Documentos' vs 'Documents').
        Requer que 'site_url' esteja definido.
        """
        if not self.site_url:
            raise ValueError("Defina 'site_url' para listar bibliotecas do site.")
        site = self._get_site_by_url(self.site_url)
        drives = self._list_site_drives(site["id"])
        return [d.get("displayName") or d.get("name") for d in drives if d.get("name") or d.get("displayName")]