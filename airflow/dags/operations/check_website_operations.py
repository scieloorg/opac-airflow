import os
import logging
import shutil
from pathlib import Path

Logger = logging.getLogger(__name__)


def check_website_uri_list(uri_list_file_path, website_url_list, report_dir):
    """
    Verifica o acesso de cada item da `uri_list_file_path`
    Exemplo de seu conteúdo:
        /scielo.php?script=sci_serial&pid=0001-3765
        /scielo.php?script=sci_issues&pid=0001-3765
        /scielo.php?script=sci_issuetoc&pid=0001-376520200005
        /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
    """
    Logger.debug("check_website_uri_list IN")

    Logger.debug("check_website_uri_list OUT")
