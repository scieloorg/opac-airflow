import logging
from datetime import datetime
from re import match
from typing import Callable, Iterable, Generator, Dict, List, Tuple

from airflow.models import Variable

import requests
import mongoengine
from lxml import etree as et
from opac_schema.v1 import models

import common.hooks as hooks
from operations.exceptions import InvalidOrderValueError
from operations.docs_utils import (
    get_bundle_id,
)

from common.sps_package import (
    SPS_Package,
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
    fetch_document_xml: callable = None,
    fetch_documents_manifest: callable = None,
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
        fetch_document_xml (callable): Função para obter o XML do Kernel caso
        fetch_document_xml (callable): Função para obter o JSON Manifest do Kernel caso
        necessário.

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
            _nestget(data, "article_meta", 0,
                     f"scielo_pid_v{version}", 0, default=None)
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
                    "%s%s, %s"
                    % (
                        _nestget(contrib, "contrib_surname", 0),
                        " " + _nestget(contrib, "contrib_suffix",
                                       0) if _nestget(contrib, "contrib_suffix", 0) else "",
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
                author_dict = {}

                author_dict['name'] = "%s%s, %s" % (
                    _nestget(contrib, "contrib_surname", 0),
                    " " + _nestget(contrib, "contrib_suffix",
                                   0) if _nestget(contrib, "contrib_suffix", 0) else "",
                    _nestget(contrib, "contrib_given_names", 0),
                )

                if _nestget(contrib, "contrib_orcid", 0):
                    author_dict['orcid'] = _nestget(
                        contrib, "contrib_orcid", 0)

                aff = _get_author_affiliation(
                    data, _nestget(contrib, "xref_aff", 0))

                if aff:
                    author_dict['affiliation'] = aff

                authors.append(models.AuthorMeta(**author_dict))

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

    def _update_related_articles(article, related_dict):
        """
        Atualiza os documentos relacionados.

        Nesse método será realizado uma atualização no related_articles de
        ambos os documento ou seja ``["correction", "retraction", "addendum",] -> documento``
        quando ``documento -> ["correction", "retraction", "addendum",]``.

        Será necessário uma pesquisa na base de dados do OPAC para obter o
        pid_v3 dos documentos relacionado para que seja possível armazena-lo
        nessa relação.

        article = A instância corrente de models.Article(Artigo sendo processado)

        related_dict = {
                        "doi" : "10.1590/S0103-50532006000200015",
                        "related_type" : "retraction"
                        }

        Está sendo alterado o atributo related_articles do ``article``
        """

        related_article = None

        related_doi = related_dict.get('doi')

        article_data = {
            "ref_id": article._id,
            "doi": article.doi,
            "related_type": article.type,
        }

        if related_doi:
            try:
                # Busca por DOIs com maiúsculo e minúsculo ``doi__iexact``
                related_article = models.Article.objects.get(
                    doi__iexact=related_doi, is_public=True)
            except models.Article.MultipleObjectsReturned as ex:
                articles = models.Article.objects.filter(
                    doi=related_doi, is_public=True)

                logging.info("Foram encontrados na base de dados do site mais de 1 artigo com o DOI: %s. Lista de ID de artigos encontrados: %s" % (
                    related_doi, [d.id for d in articles]))

                # Quando existe mais de um registro no relacionamento, consideramos o primeiro encontrado.
                first_found = articles[0]

                logging.info(
                    "Para essa relação foi considerado o primeiro encontrado, artigo com id: %s" % first_found.id)
                related_article = first_found
            except models.Article.DoesNotExist as ex:
                logging.error("Não foi possível encontrar na base de dados do site o artigo com DOI: %s, portanto, não foi possível atualiza o related_articles do relacionado, com os dados: %s, erro: %s" % (
                    related_doi, article_data, ex))

            if related_article:

                related_article_model = models.RelatedArticle(**article_data)

                # Garante a unicidade da relação.
                if related_article_model not in related_article.related_articles:
                    # Necessário atualizar o ``related_article`` com os dados do ``article`` caso ele exista na base de dados.
                    related_article.related_articles += [related_article_model]
                    related_article.save()

                # Atualiza a referência no ``ref_id`` no dicionário de ``related_article```
                related_dict['ref_id'] = related_article._id

                article_related_model = models.RelatedArticle(
                    **related_dict)

                # Garante a unicidade da relação.
                if article_related_model not in article.related_articles:
                    article.related_articles += [article_related_model]
                    logging.info("Relacionamento entre o documento processado: %s e seu relacionado: %s, realizado com sucesso. Tipo de relação entre os documentos: %s" % (
                        article.doi, related_dict.get('doi'), related_dict.get('related_type')))

    def _get_publication_date_by_type(publication_dates, date_type="pub",
                                      reverse_date=True):
        """
        Obtém a lista de datas de publicação do /front do kernel,
        no seguinte formato, exemplo:

        [{'text': ['2022'],
          'pub_type': [],
          'pub_format': ['electronic'],
          'date_type': ['collection'],
          'day': [],
          'month': [],
          'year': ['2022'],
          'season': []},
        {'text': ['02 02 2022'],
         'pub_type': [],
         'pub_format': ['electronic'],
         'date_type': ['pub'],
         'day': ['02'],
         'month': ['02'],
         'year': ['2022'],
         'season': []}]

         Retorna a data considerando a chave o tipo `date_type`.

         Return a string.
        """
        def _check_date_format(date_string, format="%Y-%m-%d"):
            """
            Check if date as string is a expected format.
            """
            try:
                return datetime.strptime(date_string, format).strftime(format)
            except ValueError:
                logging.info(
                    "The date isnt in a well format, the correct format: %s" % format)

            return date_string

        try:
            formed_date = ""
            for pubdate in publication_dates or []:
                if date_type in pubdate.get('date_type'):
                    pubdate_list = [_nestget(pubdate, 'day', 0),
                                    _nestget(pubdate, 'month', 0),
                                    _nestget(pubdate, 'year', 0)]
                    if reverse_date:
                        pubdate_list.reverse()
                    formed_date = "-".join([pub for pub in pubdate_list if pub])
            return _check_date_format(formed_date) if reverse_date else _check_date_format(formed_date, "%d-%m-%Y")
        except (IndexError, AttributeError):
            raise KernelFrontHasNoPubYearError(
                "Missing publication date type: {} in list of dates: {}".format(date_type, publication_dates))

    def _get_related_articles(sps_package):
        """
        Obtém a lista de documentos relacionados do XML e atualiza os
        documentos dessa realação.

        Tag no XML que representa essa relação:
            <related-article ext-link-type="doi" id="ra1"
            related-article-type="corrected-article"
            xlink:href="10.1590/S0103-50532006000200015"/>
        """

        for related_dict in sps_package.related_articles:
            _update_related_articles(article, related_dict)

    def _update_suppl_material(document_id, filename, url):
        """
        Atualiza os material suplementar.

        Return a suplementary material dict.

            {
                "url" : "https://minio.scielo.br/documentstore/2237-9622/d6DyD7CHXbpTJbLq7NQQNdq/5d88e2211c5357e2a9d8caeac2170f4f3d1305d1.pdf"
                "filename": "suppl01.pdf"
            }
        """

        suppl_data = {
            "url": url,
            "filename": filename
        }

        mat_suppl_entity = models.MatSuppl(**suppl_data)

        try:
            # Verifica se é uma atualização.
            _article = models.Article.objects.get(_id=document_id)
        except models.Article.DoesNotExist as ex:
            # Caso não seja uma atualização
            return models.MatSuppl(**suppl_data)
        else:
            # É uma atualização
            # Mantém a unicidade da atualização do material suplementar
            if mat_suppl_entity not in _article.mat_suppl:
                _article.mat_suppl += [mat_suppl_entity]
                return _article.mat_suppl
            else:
                return _article.mat_suppl

    def _get_suppl_material(article, json):
        """
        Obtém a lista de material suplementar do JSON do Manifest do Kernel e caso existe atualiza a entidade MatSuppl.

        Tags no XML o material suplementar: ["inline-supplementary-material", "supplementary-material"]:
            <inline-supplementary-material xlink:href="1678-8060-mioc-116-e210259-s.pdf">Supplementary data
            </inline-supplementary-material>
            <supplementary-material id="suppl01" mimetype="application" mime-subtype="pdf" xlink:href="1234-5678-rctb-45-05-0110-suppl01.pdf"/>
        """
        # check if exist a supplementary_material
        logging.info("Checking if exists supplementary material....")

        assets = _nestget(json, "versions", 0, "assets")
        suppls = [k for k in assets.keys() if 'suppl' in k]

        if any(suppls):
            logging.info("Exists supplementary material: %s" %
                         (' '.join(suppls)))
            for key, asset in assets.items():
                if key in suppls:
                    return _update_suppl_material(article,
                                                  filename=key, url=_nestget(asset, 0, 1))

    def _get_doi_with_lang(article, sps_package):
        """
        Obtém a lista de DOIs com idiomas do XML e atualiza o artigo,
        acrescetando o doi do idioma original.

        Trecho do XML em que obtemos essa dado:
        <sub-article article-type="translation" id="s1" xml:lang="en">
            <front-stub>
                <article-id pub-id-type="doi">10.1590/S0034-759020210302x</article-id>
                <article-categories>
        """

        doi_with_lang = sps_package.doi_with_lang

        # Adiciona o DOI do idioma original.
        doi_with_lang.append(
            {'doi': article.doi, 'language': article.original_language})

        return doi_with_lang

    article.authors = list(_get_article_authors(data))
    article.authors_meta = _get_article_authors_meta(data)
    article.languages = list(_get_languages(data))
    article.translated_titles = list(_get_translated_titles(data))
    article.sections = list(_get_translated_sections(data))
    article.abstracts = list(_get_abstracts(data))
    article.keywords = list(_get_keywords(data))

    article.abstract_languages = [
        abstract["language"] for abstract in article.abstracts
    ]

    article.original_language = _get_original_language(data)
    publications_date = _nestget(data, "pub_date")

    if publications_date:
        formed_publication_date = _get_publication_date_by_type(
            publications_date, "pub")
        article.publication_date = formed_publication_date

    article.type = _nestget(data, "article", 0, "type", 0)

    # Dados de localização
    article.elocation = _nestget(data, "article_meta", 0, "pub_elocation", 0)
    article.fpage = _nestget(data, "article_meta", 0, "pub_fpage", 0)
    article.fpage_sequence = _nestget(
        data, "article_meta", 0, "pub_fpage_seq", 0)
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

    # Cadastra o material suplementar
    if fetch_documents_manifest:
        json = fetch_documents_manifest(document_id)
        mat_suppl = _get_suppl_material(document_id, json)

        try:
            article.mat_suppl = mat_suppl
            article.save()
        except mongoengine.errors.ValidationError as ex:
            logging.error("Erro ao tentar salvar o material supplementar do artigo!, error: %s", ex)
            logging.error("Conteúdo retornado na função que obtém a lista de suplemento: %s", mat_suppl)
            article.mat_suppl = []

    xml = fetch_document_xml(document_id)

    try:
        etree_xml = et.XML(xml)
        sps_package = SPS_Package(etree_xml)
    except ValueError as ex:
        logging.error(
            "Erro ao tentar analisar(parser) do XML, erro: %s", ex)
    else:

        # Verifica se o XML contém DOI com idioma
        doi_with_langs = _get_doi_with_lang(article, sps_package)
        article.doi_with_lang = [models.DOIWithLang(**doi)
                                 for doi in doi_with_langs]

        # Se for uma errata ou retratação ou adendo ou comentário de artigo.
        if article.type in ["correction", "retraction", "addendum", "article-commentary"]:
            # Obtém o XML da errada no kernel
            _get_related_articles(sps_package)

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
    fetch_document_xml: callable = None,
    fetch_documents_manifest: callable = None,
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
        fetch_document_xml (callable): função que recupera XML
            do documento a partir da API do Kernel.

    Returns:
        List[str] orphans: Lista contendo todos os identificadores dos
            documentos que não puderam ser registrados na base de dados do
            OPAC.
    """

    orphans = []
    failed_documents = Variable.get("failed_documents", default_var=[],
                                   deserialize_json=True)

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
                fetch_document_xml,
                fetch_documents_manifest
            )
            document.save()
            logging.info("ARTICLE saved %s %s" % (document_id, issue_id))
            logging.info(
                "Link to article on OPAC: https://www.scielo.br/j/%s/a/%s" % (document.journal.acronym, document_id))

            # Remove itens que estava com falha.
            if document_id in failed_documents:
                failed_documents.remove(document_id)

        except InvalidOrderValueError as e:
            failed_documents.append(document_id)
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
            failed_documents.append(document_id)
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
            failed_documents.append(document_id)
            logging.error(
                "Could not register document '%s'. "
                "Unexpected error '%s'.",
                document_id,
                str(exc),
            )

    return (list(set(orphans)), list(set(failed_documents)))


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

    new_doc = models.Article.objects(_id=document_id)
    try:
        new_doc = new_doc[0]
        new_title = new_doc.title
        new_issue = new_doc.issue
    except (IndexError, TypeError, ValueError, AttributeError) as e:
        logging.info(
            "Unpublished repeated document %s. %s" %
            (document_id, e))
        return
    except Exception as e:
        logging.info(
            "Unpublished repeated document %s. Unexpected: %s" %
            (document_id, e))
        return

    pids = set()
    for doc in docs:
        if doc._id == document_id:
            continue
        if doc.title != new_title:
            continue
        if doc.issue != new_issue and not doc.issue.endswith("aop"):
            continue

        logging.info("Repeated document %s / %s / %s / %s" %
                     (doc._id, doc.pid, doc.aop_pid, str(doc.scielo_pids)))
        # obtém os pids
        pids |= set(doc.scielo_pids and (doc.scielo_pids.get("other") or []))
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
