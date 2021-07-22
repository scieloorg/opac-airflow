import logging
from datetime import datetime
from re import match
from typing import Iterable, Generator, Dict, List, Tuple

import requests
from opac_schema.v1 import models

import common.hooks as hooks
from operations.exceptions import InvalidOrderValueError
from operations.docs_utils import (
    get_bundle_id,
)
from common.sps_package import (
    extract_number_and_supplment_from_issue_element,
)

class KernelFrontHasNoPubYearError(Exception):
    ...


def _nestget(data, *path, default=""):
    """Obtém valores de list ou dicionários."""
    for key_or_index in path:
        try:
            data = data[key_or_index]
        except (KeyError, IndexError):
            return default
    return data


def _get_main_article_title(data):
    try:
        lang = _nestget(data, "article", 0, "lang", 0)
        return data['display_format']['article_title'][lang]
    except KeyError:
        return _nestget(data, "article_meta", 0, "article_title", 0)


def _get_bundle_pub_year(publication_dates):
    """
    Retorna o ano de publicação do fascículo a partir dos dados pub_date
    provenientes do kernel front
    """
    try:
        pubdates = {}
        for pubdate in publication_dates or []:
            pub_date_type = pubdate.get("date_type") or pubdate.get("pub_type")
            pub_year = pubdate.get("year")
            if pub_year and pub_date_type:
                pubdates[pub_date_type[0]] = pub_year[0]
        return pubdates.get("collection") or list(pubdates.values())[0]
    except (IndexError, AttributeError):
        raise KernelFrontHasNoPubYearError(
            "Missing publication year in: {}".format(publication_dates))


def _get_bundle_id(kernel_front_data):
    """
    Retorna o bundle_id do fascículo a partir dos dados
    provenientes do kernel front
    """
    article_meta = _nestget(kernel_front_data, "article_meta", 0)

    issue = _nestget(article_meta, "pub_issue", 0)
    number, supplement = extract_number_and_supplment_from_issue_element(issue)
    volume = _nestget(article_meta, "pub_volume", 0)
    scielo_pid_v2 = _nestget(article_meta, "scielo_pid_v2", 0)
    issn_id = scielo_pid_v2[1:10]
    year = _get_bundle_pub_year(_nestget(kernel_front_data, "pub_date"))
    return get_bundle_id(
        issn_id, year, volume or None, number or None, supplement or None)


def ArticleFactory(
    document_id: str,
    data: dict,
    issue_id: str,
    document_order: int,
    document_xml_url: str,
    repeated_doc_pids=None,
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

        if issue_id is None:
            issue_id = article.issue._id
    except models.Article.DoesNotExist:
        article = models.Article()

    # atualiza status
    article.is_public = True

    # Dados principais
    article.title = _get_main_article_title(data)
    article.section = _nestget(data, "article_meta", 0, "pub_subject", 0)
    article.abstract = _nestget(data, "article_meta", 0, "abstract", 0)

    # Identificadores
    article._id = document_id
    article.aid = document_id
    # Lista de SciELO PIDs dentro de article_meta
    scielo_pids = [
        (
            f"v{version}",
            _nestget(data, "article_meta", 0, f"scielo_pid_v{version}", 0, default=None)
        )
        for version in range(1, 4)
    ]
    article.scielo_pids = {
        version: value for version, value in scielo_pids if value is not None
    }

    # insere outros tipos de PIDs/IDs em `scielo_ids['other']`
    article_publisher_id = _nestget(
        data, "article_meta", 0, "article_publisher_id") or []
    repeated_doc_pids = repeated_doc_pids or []
    repeated_doc_pids = list(set(repeated_doc_pids + article_publisher_id))
    if repeated_doc_pids:
        article.scielo_pids.update({"other": repeated_doc_pids})

    article.aop_pid = _nestget(data, "article_meta", 0, "previous_pid", 0)
    article.pid = article.scielo_pids.get("v2")

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

    def _get_author_affiliation(data, xref_aff_id):
        """Recupera a afiliação ``institution_orgname`` de xref_aff_id"""

        for aff in _nestget(data, "aff"):
            if _nestget(aff, "aff_id", 0) == xref_aff_id:
                return _nestget(aff, "institution_orgname", 0)

    def _get_article_authors_meta(data):
        """Recupera a lista de autores do artigo para popular opac_schema.AuthorMeta,
        contendo a afiliação e orcid"""

        authors = []

        for contrib in _nestget(data, "contrib"):
            if _nestget(contrib, "contrib_type", 0) in AUTHOR_CONTRIB_TYPES:
                authors.append(
                    {'name':
                        "%s, %s"
                        % (
                            _nestget(contrib, "contrib_surname", 0),
                            _nestget(contrib, "contrib_given_names", 0),
                        ),
                     'orcid': _nestget(contrib, "contrib_orcid", 0),
                     'affiliation': _get_author_affiliation(data, _nestget(contrib, "xref_aff", 0))
                    }
                )

        return authors

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
        try:
            _lang = _get_original_language(data)
            for lang, title in data['display_format']['article_title'].items():
                if _lang != lang:
                    yield models.TranslatedTitle(
                        **{
                            "name": title,
                            "language": lang,
                        }
                    )
        except KeyError:
            for sub_article in _nestget(data, "sub_article"):
                yield models.TranslatedTitle(
                    **{
                        "name": _nestget(
                            sub_article, "article_meta", 0, "article_title", 0
                        ),
                        "language": _nestget(
                            sub_article, "article", 0, "lang", 0),
                    }
                )

    def _get_translated_sections(data: dict) -> List[models.TranslatedSection]:
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

        abstracts = []

        # Abstract do texto original
        if len(_nestget(data, "article_meta", 0, "abstract", 0)) > 0:
            abstracts.append(
                models.Abstract(
                    **{
                        "text": _nestget(data, "article_meta", 0, "abstract", 0),
                        "language": _get_original_language(data),
                    }
                )
            )

        # Trans abstracts
        abstracts += [
            models.Abstract(
                **{
                    "text": _nestget(trans_abstract, "text", 0),
                    "language": _nestget(trans_abstract, "lang", 0),
                }
            )
            for trans_abstract in data.get("trans_abstract", [])
            if trans_abstract and _nestget(trans_abstract, "text", 0)
        ]

        # Abstracts de sub-article
        abstracts += [
            models.Abstract(
                **{
                    "text": _nestget(sub_article, "article_meta", 0, "abstract", 0),
                    "language": _nestget(sub_article, "article", 0, "lang", 0),
                }
            )
            for sub_article in _nestget(data, "sub_article")
            if len(_nestget(sub_article, "article_meta", 0, "abstract", 0)) > 0
        ]

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

    def _get_order(document_order, pid_v2):
        try:
            return int(document_order)
        except (ValueError, TypeError):
            order_err_msg = (
                "'{}' is not a valid value for "
                "'article.order'".format(document_order)
            )
            try:
                document_order = int(pid_v2[-5:])
                logging.exception(
                    "{}. It was set '{} (the last 5 digits of PID v2)' to "
                    "'article.order'".format(order_err_msg, document_order))
                return document_order
            except (ValueError, TypeError):
                raise InvalidOrderValueError(order_err_msg)

    article.authors = list(_get_article_authors(data))
    article.authors_meta = _get_article_authors_meta(data)
    article.languages = list(_get_languages(data))
    article.translated_titles = list(_get_translated_titles(data))
    article.trans_sections = list(_get_translated_sections(data))
    article.abstracts = list(_get_abstracts(data))
    article.keywords = list(_get_keywords(data))

    article.abstract_languages = [
        abstract["language"] for abstract in article.abstracts
    ]

    article.original_language = _get_original_language(data)
    publication_date = _nestget(data, "pub_date", 0, "text", 0)

    if publication_date:
        publication_date_list = publication_date.split(" ")
        publication_date_list.reverse()
        publication_date = "-".join(publication_date_list)

    article.publication_date = publication_date

    article.type = _nestget(data, "article", 0, "type", 0)

    # Dados de localização
    article.elocation = _nestget(data, "article_meta", 0, "pub_elocation", 0)
    article.fpage = _nestget(data, "article_meta", 0, "pub_fpage", 0)
    article.fpage_sequence = _nestget(data, "article_meta", 0, "pub_fpage_seq", 0)
    article.lpage = _nestget(data, "article_meta", 0, "pub_lpage", 0)

    if article.issue is not None and article.issue.number == "ahead":
        if article.aop_url_segs is None:
            url_segs = {
                "url_seg_article": article.url_segment,
                "url_seg_issue": article.issue.url_segment,
            }
            article.aop_url_segs = models.AOPUrlSegments(**url_segs)

    # Issue vinculada
    issue = models.Issue.objects.get(_id=issue_id)

    logging.info("ISSUE %s" % str(issue))
    logging.info("ARTICLE.ISSUE %s" % str(article.issue))
    logging.info("ARTICLE.AOP_PID %s" % str(article.aop_pid))
    logging.info("ARTICLE.PID %s" % str(article.pid))

    article.issue = issue
    article.journal = issue.journal
    article.order = _get_order(document_order, article.pid)
    article.xml = document_xml_url

    # Campo de compatibilidade do OPAC
    article.htmls = [{"lang": lang} for lang in _get_languages(data)]

    article.created = article.created or datetime.utcnow().isoformat()
    article.updated = datetime.utcnow().isoformat()

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
            document_front = fetch_document_front(document_id)
            issue_id, item = get_relation_data(document_id, document_front)
            doi = (
                _get_doi_from_kernel(document_front) or
                _get_doi_from_website(document_id)
            )
            repeated_doc_pids = _unpublish_repeated_documents(document_id, doi)
            document_xml_url = "{base_url}documents/{document_id}".format(
                base_url=BASE_URL, document_id=document_id
            )
            document = article_factory(
                document_id,
                document_front,
                issue_id,
                item.get("order"),
                document_xml_url,
                repeated_doc_pids,
            )
            document.save()
            logging.info("ARTICLE saved %s %s" % (document_id, issue_id))
        except InvalidOrderValueError as e:
            logging.error(
                "Could not register document %s. "
                "%s",
                document_id,
                str(e),
            )
        except models.Issue.DoesNotExist:
            orphans.append(document_id)
            logging.error(
                "Could not register document %s. "
                "Issue '%s' can't be found in the website database.",
                document_id,
                issue_id,
            )
        except requests.exceptions.HTTPError as exc:
            logging.error(
                "Could not register document '%s'. "
                "The code '%s' was returned during the request to '%s'.",
                document_id,
                exc.response.status_code,
                exc.response.url,
            )
            if exc.response.status_code == 410:
                # GONE
                _unpublish_deleted_document(document_id)
        except Exception as exc:
            logging.error(
                "Could not register document '%s'. "
                "Unexpected error '%s'.",
                document_id,
                str(exc),
            )

    return list(set(orphans))


def _unpublish_deleted_document(document_id):
    """
    Despublica documento no site se estiver deleted no kernel
    """
    logging.info(
        "Unpublish document '%s' because it does not exist at kernel",
        document_id,
    )
    try:
        article = models.Article.objects.get(_id=document_id)
    except models.Article.DoesNotExist:
        logging.info(
            "'%s' does not exist in Website database",
            document_id,
        )
        return
    try:
        article.is_public = False
        doc.unpublish_reason = "unavailable at kernel"
        article.save()
        _unpublish_repeated_documents(document_id, article.doi)
    except Exception as e:
        logging.error(
            "Could not unpublish document '%s'. %s" % (
                document_id,
                e,
            )
        )


def _unpublish_repeated_documents(document_id, doi):
    """
    Identifica documentos _repetidos_ usando o `doi`
    Obtém os pids dos documentos _repetidos_ para serem ingressados no registro
    que representará o documento
    """
    if not doi:
        return None

    try:
        docs = models.Article.objects(doi=doi)
    except models.Article.DoesNotExist:
        logging.info("No registered article with doi='%s'" % doi)
    except Exception as e:
        logging.info("Error getting documents by doi='%s': %s" % (doi, str(e)))
        return None

    pids = set()
    for doc in docs:
        if doc._id == document_id:
            continue
        logging.info("Repeated document %s / %s / %s / %s" %
                     (doc._id, doc.pid, doc.aop_pid, str(doc.scielo_pids)))
        # obtém os pids
        pids |= set(doc.scielo_pids and doc.scielo_pids.values() or [])
        pids |= set([i for i in [doc._id, doc.pid, doc.aop_pid] if i])

        try:
            # despublica
            doc.is_public = False
            doc.unpublish_reason = "repeated {}".format(document_id)
            doc.save()
        except Exception as e:
            logging.info(
                "Error unpublishing repeated document %s: %s" %
                (doc._id, str(e)))
    return list(pids)


def _get_doi_from_kernel(document_front):
    """
    Obtém o doi do documento pelo registro no kernel
    """
    logging.info("Get doi from kernel")
    return _nestget(document_front, "article_meta", 0, "article_doi", 0)


def _get_doi_from_website(document_id):
    """
    Obtém o doi do documento pelo registro no site.
    """
    logging.info("Get doi from website")
    try:
        article = models.Article.objects.get(_id=document_id)
    except models.Article.DoesNotExist:
        logging.info("%s is not registered" % document_id)
    else:
        return article.doi


def ArticleRenditionFactory(article_id: str, data: List[dict]) -> models.Article:
    """Recupera uma instância de artigo a partir de um article_id e popula seus
    assets a partir dos dados de entrada.

    A partir do article_id uma instância de Artigo é recuperada da base OPAC e
    seus assets são populados. Se o Artigo não existir na base OPAC a exceção
    models.ArticleDoesNotExists é lançada.

    Args:
        article_id (str): Identificador do artigo a ser recuperado
        data (List[dict]): Lista de renditions do artigo

    Returns:
        models.Article: Artigo recuperado e atualizado com uma nova lista de assets."""

    article = models.Article.objects.get(_id=article_id)
    article.pdfs = [
        {
            "lang": rendition["lang"],
            "url": rendition["url"],
            "filename": rendition["filename"],
            "type": "pdf",
        }
        for rendition in data
        if rendition["mimetype"] == "application/pdf"
    ]
    return article


def try_register_documents_renditions(
    documents: List[str],
    get_rendition_data: callable,
    article_rendition_factory: callable,
) -> List[str]:
    """Registra as manifestações de documentos na base de dados do OPAC

    As manifestações que não puderem ser registradas serão consideradas como
    órfãos.

    Args:
        documents (Iterable): iterável com os identicadores dos documentos
            a serem registrados.
        get_rendition_data (callable): Recupera os dados de manifestações
            de um documento.
        article_rendition_factory (callable): Recupera uma instância de
            artigo da base OPAC e popula as suas manifestações de acordo
            com os dados apresentados.

    Returns:
        List[str] orphans: Lista contendo todos os identificadores dos
            documentos em que as manifestações que não puderam ser registradas
            na base de dados do OPAC.
    """

    orphans = []

    for document in documents:
        try:
            data = get_rendition_data(document)
            article = article_rendition_factory(document, data)
            article.save()
        except models.Article.DoesNotExist:
            logging.error(
                "Could not save renditions for document '%s' because "
                "it does not exist in website database.",
                document,
            )
            orphans.append(document)

    return list(set(orphans))
