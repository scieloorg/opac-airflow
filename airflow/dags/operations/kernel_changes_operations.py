import logging
from typing import Iterable, Generator, Dict, List, Tuple

from opac_schema.v1 import models

import common.hooks as hooks


def ArticleFactory(
    document_id: str,
    data: dict,
    issue_id: str,
    document_order: int,
    document_xml_url: str,
) -> models.Article:
    """Cria uma instância de artigo a partir dos dados de entrada.

    Os dados do parâmetro `data` são adaptados ao formato exigido pelo
    modelo Article do OPAC Schema.

    Args:
        document_id (str): Identificador do documento
        data (dict): Estrutura contendo o `front` do documento.
        issue_id (str): Identificador de issue.
        document_order (int): Posição do artigo.
        document_xml_url (str): URL do XML do artigo

    Returns:
        models.Article: Instância de um artigo próprio do modelo de dados do
            OPAC.
    """

    def _nestget(data, *path, default=""):
        """Obtém valores de list ou dicionários."""
        for key_or_index in path:
            try:
                data = data[key_or_index]
            except (KeyError, IndexError):
                return default
        return data

    AUTHOR_CONTRIB_TYPES = (
        "author",
        "editor",
        "organizer",
        "translator",
        "autor",
        "compiler",
    )

    try:
        article = models.Article.objects.get(_id=document_id)
    except models.Article.DoesNotExist:
        article = models.Article()

    # Dados principais
    article.title = _nestget(data, "article_meta", 0, "article_title", 0)
    article.section = _nestget(data, "article_meta", 0, "pub_subject", 0)
    article.abstract = _nestget(data, "article_meta", 0, "abstract", 0)

    # Identificadores
    article._id = _nestget(data, "article_meta", 0, "article_publisher_id", 0)
    article.aid = _nestget(data, "article_meta", 0, "article_publisher_id", 0)
    article.pid = _nestget(data, "article_meta", 0, "article_publisher_id", 1)
    article.doi = _nestget(data, "article_meta", 0, "article_doi", 0)

    def _get_article_authors(data) -> Generator:
        """Recupera a lista de autores do artigo"""

        for contrib in _nestget(data, "contrib"):
            if _nestget(contrib, "contrib_type", 0) in AUTHOR_CONTRIB_TYPES:
                yield (
                    "%s, %s"
                    % (
                        _nestget(contrib, "contrib_surname", 0),
                        _nestget(contrib, "contrib_given_names", 0),
                    )
                )

    def _get_original_language(data: dict) -> str:
        return _nestget(data, "article", 0, "lang", 0)

    def _get_languages(data: dict) -> List[str]:
        """Recupera a lista de idiomas em que o artigo foi publicado"""

        languages = [_get_original_language(data)]

        for sub_article in _nestget(data, "sub_article"):
            languages.append(_nestget(sub_article, "article", 0, "lang", 0))

        return languages

    def _get_translated_titles(data: dict) -> Generator:
        """Recupera a lista de títulos do artigo"""

        for sub_article in _nestget(data, "sub_article"):
            yield models.TranslatedTitle(
                **{
                    "name": _nestget(
                        sub_article, "article_meta", 0, "article_title", 0
                    ),
                    "language": _nestget(sub_article, "article", 0, "lang", 0),
                }
            )

    def _get_translaed_sections(data: dict) -> List[models.TranslatedSection]:
        """Recupera a lista de seções traduzidas a partir do document front"""

        sections = [
            models.TranslatedSection(
                **{
                    "name": _nestget(data, "article_meta", 0, "pub_subject", 0),
                    "language": _get_original_language(data),
                }
            )
        ]

        for sub_article in _nestget(data, "sub_article"):
            sections.append(
                models.TranslatedSection(
                    **{
                        "name": _nestget(
                            sub_article, "article_meta", 0, "pub_subject", 0
                        ),
                        "language": _nestget(sub_article, "article", 0, "lang", 0),
                    }
                )
            )

        return sections

    def _get_abstracts(data: dict) -> List[models.Abstract]:
        """Recupera todos os abstracts do artigo"""

        abstracts = [
            models.Abstract(
                **{
                    "text": _nestget(data, "article_meta", 0, "abstract", 0),
                    "language": _get_original_language(data),
                }
            )
        ]

        for trans_abstract in data.get("trans_abstract", []):
            abstracts.append(
                models.Abstract(
                    **{
                        "text": _nestget(trans_abstract, "text", 0),
                        "language": _nestget(trans_abstract, "lang", 0),
                    }
                )
            )

        for sub_article in _nestget(data, "sub_article"):
            abstracts.append(
                models.Abstract(
                    **{
                        "text": _nestget(sub_article, "article_meta", 0, "abstract", 0),
                        "language": _nestget(sub_article, "article", 0, "lang", 0),
                    }
                )
            )

        return abstracts

    def _get_keywords(data: dict) -> List[models.ArticleKeyword]:
        """Retorna a lista de palavras chaves do artigo e dos
        seus sub articles"""

        keywords = [
            models.ArticleKeyword(
                **{
                    "keywords": _nestget(kwd_group, "kwd", default=[]),
                    "language": _nestget(kwd_group, "lang", 0),
                }
            )
            for kwd_group in _nestget(data, "kwd_group", default=[])
        ]

        for sub_article in _nestget(data, "sub_article"):
            [
                keywords.append(
                    models.ArticleKeyword(
                        **{
                            "keywords": _nestget(kwd_group, "kwd", default=[]),
                            "language": _nestget(kwd_group, "lang", 0),
                        }
                    )
                )
                for kwd_group in _nestget(sub_article, "kwd_group", default=[])
            ]

        return keywords

    article.authors = list(_get_article_authors(data))
    article.languages = list(_get_languages(data))
    article.translated_titles = list(_get_translated_titles(data))
    article.trans_sections = list(_get_translaed_sections(data))
    article.abstracts = list(_get_abstracts(data))
    article.keywords = list(_get_keywords(data))

    article.abstract_languages = [
        abstract["language"] for abstract in article.abstracts
    ]

    article.original_language = _get_original_language(data)
    article.publication_date = _nestget(data, "pub_date", 0, "text", 0)

    article.type = _nestget(data, "article", 0, "type", 0)

    # Dados de localização
    article.elocation = _nestget(data, "article_meta", 0, "pub_elocation", 0)
    article.fpage = _nestget(data, "article_meta", 0, "pub_fpage", 0)
    article.fpage_sequence = _nestget(data, "article_meta", 0, "pub_fpage_seq", 0)
    article.lpage = _nestget(data, "article_meta", 0, "pub_lpage", 0)

    # Issue vinculada
    if issue_id:
        issue = models.Issue.objects.get(_id=issue_id)
        article.issue = issue
        article.journal = issue.journal

    if document_order:
        article.order = int(document_order)

    article.xml = document_xml_url

    return article


def try_register_documents(
    documents: Iterable,
    get_relation_data: callable,
    fetch_document_front: callable,
    article_factory: callable,
) -> List[str]:
    """Registra documentos do Kernel na base de dados do `OPAC`.

    Os documentos que não puderem ser registrados serão considerados como
    órfãos. Documentos que não possuam identificação a qual fascículo tem
    relação serão considerados órfãos.

    Args:
        documents (Iterable): iterável com os identicadores dos documentos 
            a serem registrados.
        get_relation_data (callable): função que identifica o fasículo 
            e o item de relacionamento que está sendo registrado.
        fetch_document_front (callable): função que recupera os dados de 
            `front` do documento a partir da API do Kernel.
        article_factory (callable): função que cria uma instância do modelo 
            de dados do Artigo na base do OPAC.

    Returns:
        List[str] orphans: Lista contendo todos os identificadores dos
            documentos que não puderam ser registrados na base de dados do
            OPAC.
    """

    orphans = []

    # Para capturarmos a URL base é necessário que o hook tenha sido utilizado
    # ao menos uma vez.
    BASE_URL = hooks.KERNEL_HOOK_BASE.run(
        endpoint="", extra_options={"timeout": 1, "check_response": False}
    ).url

    for document_id in documents:
        try:
            issue_id, item = get_relation_data(document_id)
            document_front = fetch_document_front(document_id)
            document_xml_url = "{base_url}documents/{document_id}".format(
                base_url=BASE_URL, document_id=document_id
            )
            document = article_factory(
                document_id,
                document_front,
                issue_id,
                item.get("order"),
                document_xml_url,
            )
            document.save()
        except (models.Issue.DoesNotExist, ValueError):
            orphans.append(document_id)
            logging.info(
                "Could not possible to register the document %s. "
                "Probably the issue that is related to it does not exist." % document_id
            )

    return list(set(orphans))
