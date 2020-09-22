import os
import itertools
import logging

from lxml import etree


logger = logging.getLogger(__name__)


def parse_date(_date):
    def format(value):
        if value and value.isdigit():
            return value.zfill(2)
        return value or ""

    if _date is not None:
        return tuple(
            [format(_date.findtext(item) or "") for item in ["year", "month", "day"]]
        )
    return "", "", ""


def parse_value(value):
    value = value.lower()
    if value.isdigit():
        return value.zfill(2)
    if "spe" in value:
        return "spe"
    if "sup" in value:
        return "s"
    return value


def parse_issue(issue):
    issue = " ".join([item for item in issue.split()])
    parts = issue.split()
    parts = [parse_value(item) for item in parts]
    s = "-".join(parts)
    s = s.replace("spe-", "spe")
    s = s.replace("s-", "s")
    if s.endswith("s"):
        s += "0"
    return s


class SPS_Package:
    def __init__(self, xmltree, _original_asset_name_prefix=None):
        self.xmltree = xmltree
        self._original_asset_name_prefix = _original_asset_name_prefix

    @property
    def article_meta(self):
        return self.xmltree.find(".//article-meta")

    @property
    def xmltree(self):
        return self._xmltree

    @xmltree.setter
    def xmltree(self, value):
        try:
            etree.tostring(value)
        except TypeError:
            raise
        else:
            self._xmltree = value

    @property
    def issn(self):
        return (
            self.xmltree.findtext('.//issn[@pub-type="epub"]')
            or self.xmltree.findtext('.//issn[@pub-type="ppub"]')
            or self.xmltree.findtext(".//issn")
        )

    @property
    def acron(self):
        return self.xmltree.findtext('.//journal-id[@journal-id-type="publisher-id"]')


    @property
    def journal_meta(self):
        data = []
        issns = [
            self.xmltree.findtext('.//issn[@pub-type="epub"]'),
            self.xmltree.findtext('.//issn[@pub-type="ppub"]'),
            self.xmltree.findtext(".//issn"),
        ]
        for issn_type, issn in zip(["eissn", "pissn", "issn"], issns):
            if issn:
                data.append((issn_type, issn))
        if self.acron:
            data.append(("acron", self.acron))
        return data

    @property
    def document_bundle_pub_year(self):
        if self.article_meta is not None:
            xpaths = (
                'pub-date[@pub-type="collection"]',
                'pub-date[@date-type="collection"]',
                'pub-date[@pub-type="epub-pub"]',
                'pub-date[@pub-type="epub"]',
            )
            for xpath in xpaths:
                pubdate = self.article_meta.find(xpath)
                if pubdate is not None and pubdate.findtext("year"):
                    return pubdate.findtext("year")

    @property
    def parse_article_meta(self):
        elements = [
            "volume",
            "issue",
            "fpage",
            "lpage",
            "elocation-id",
            "pub-date",
            "article-id",
        ]
        items = []
        for elem_name in elements:
            xpath = ".//article-meta//{}".format(elem_name)
            for node in self.xmltree.findall(xpath):
                if node is not None:
                    content = node.text
                    if node.tag == "article-id":
                        elem_name = node.get("pub-id-type")
                        if elem_name == "doi":
                            if "/" in content:
                                content = content[content.find("/") + 1 :]
                    if node.tag == "issue":
                        content = parse_issue(content)
                    elif node.tag == "pub-date":
                        content = self.document_bundle_pub_year
                        elem_name = "year"
                    if content and content.isdigit() and int(content) == 0:
                        content = ""
                    if content:
                        items.append((elem_name, content))
        return items

    @property
    def package_name(self):
        if self._original_asset_name_prefix is None:
            raise ValueError(
                "SPS_Package._original_asset_name_prefix has an invalid value."
            )
        data = dict(self.parse_article_meta)
        data_labels = data.keys()
        labels = ["volume", "issue", "fpage", "lpage", "elocation-id"]
        if "volume" not in data_labels and "issue" not in data_labels:
            if "doi" in data_labels:
                data.update({"type": "ahead"})
                labels.append("type")
                labels.append("year")
                labels.append("doi")
            elif "other" in data_labels:
                data.update({"type": "ahead"})
                labels.append("type")
                labels.append("year")
                labels.append("other")
        elif (
            "fpage" not in data_labels
            and "lpage" not in data_labels
            and "elocation-id" not in data_labels
            and "doi" not in data_labels
        ):
            labels.append("other")
        items = [self.issn, self.acron]
        items += [data[k] for k in labels if k in data_labels]
        return (
            "-".join([item for item in items if item])
            or self._original_asset_name_prefix
        )

    def asset_name(self, img_filename):
        if self._original_asset_name_prefix is None:
            raise ValueError(
                "SPS_Package._original_asset_name_prefix has an invalid value."
            )
        filename, ext = os.path.splitext(self._original_asset_name_prefix)
        suffix = img_filename
        if img_filename.startswith(filename):
            suffix = img_filename[len(filename) :]
        return "-g".join([self.package_name, suffix])

    @property
    def elements_which_has_xlink_href(self):
        paths = [
            ".//graphic[@xlink:href]",
            ".//inline-graphic[@xlink:href]",
            ".//inline-supplementary-material[@xlink:href]",
            ".//media[@xlink:href]",
            ".//supplementary-material[@xlink:href]",
        ]
        iterators = [
            self.xmltree.iterfind(
                path, namespaces={"xlink": "http://www.w3.org/1999/xlink"}
            )
            for path in paths
        ]
        return itertools.chain(*iterators)

    @property
    def volume(self):
        return dict(self.parse_article_meta).get("volume")

    @property
    def number(self):
        if self.article_meta is not None:
            issue_tag = self.article_meta.find("./issue")
            if issue_tag is not None:
                issue_tag_text = issue_tag.text.strip()
                lower_value = issue_tag_text.lower()
                if "sup" in lower_value:
                    index = lower_value.find("sup")
                    if index == 0:  # starts with "s"
                        return None
                    issue_tag_text = issue_tag_text[:index].strip()
                return "".join(issue_tag_text.split())

    @property
    def supplement(self):
        issue = dict(self.parse_article_meta).get("issue")
        if issue:
            if "s" in issue and "spe" not in issue:
                if "-s" in issue:
                    return issue[issue.find("-s") + 2 :]
                if issue.startswith("s"):
                    return issue[1:]

    @property
    def year(self):
        return self.documents_bundle_pubdate[0]

    @property
    def documents_bundle_id(self):
        items = []
        data = dict(self.journal_meta)
        for label in ["eissn", "pissn", "issn"]:
            if data.get(label):
                items = [data.get(label)]
                break

        items.append(data.get("acron"))

        data = dict(self.parse_article_meta)
        if not data.get("volume") and not data.get("issue"):
            items.append("aop")
        else:
            labels = ("year", "volume", "issue")
            items.extend([data[k] for k in labels if data.get(k)])
        return "-".join(items)

    @property
    def is_only_online_publication(self):
        fpage = self.article_meta.findtext("fpage")
        if fpage and fpage.isdigit():
            fpage = int(fpage)
        if fpage:
            return False

        lpage = self.article_meta.findtext("lpage")
        if lpage and lpage.isdigit():
            lpage = int(lpage)
        if lpage:
            return False

        volume = self.article_meta.findtext("volume")
        issue = self.article_meta.findtext("issue")
        if volume or issue:
            return bool(self.article_meta.findtext("elocation-id"))

        return True

    @property
    def order(self):
        def format(value):
            if value and value.isdigit():
                return value.zfill(5)
            return value or ""

        data = dict(self.parse_article_meta)
        return format(data.get("other")) or format(data.get("fpage"))

    def _match_pubdate(self, pubdate_xpaths):
        """
        Retorna o primeiro match da lista de pubdate_xpaths
        """
        for xpath in pubdate_xpaths:
            pubdate = self.article_meta.find(xpath)
            if pubdate is not None:
                return pubdate

    @property
    def document_pubdate(self):
        xpaths = (
            'pub-date[@pub-type="epub"]',
            'pub-date[@date-type="pub"]',
            "pub-date",
        )
        return parse_date(self._match_pubdate(xpaths))

    @property
    def documents_bundle_pubdate(self):
        xpaths = (
            'pub-date[@pub-type="epub-ppub"]',
            'pub-date[@pub-type="collection"]',
            'pub-date[@date-type="collection"]',
            "pub-date",
        )
        return parse_date(self._match_pubdate(xpaths))

    @property
    def scielo_pid_v3(self):
        """The scielo id of the main document.
        """
        return self.xmltree.findtext(".//article-id[@specific-use='scielo-v3']")

    @property
    def scielo_pid_v2(self):
        """The scielo id of the main document, version 2.
        """
        return self.xmltree.findtext(".//article-id[@specific-use='scielo-v2']")

    @property
    def scielo_previous_pid(self):
        """The previous scielo id of the main document, version 2.
        """
        return self.xmltree.findtext(".//article-id[@specific-use='previous-pid']")

    @property
    def original_language(self):
        """The the main document language.
        """
        return self.xmltree.get("{http://www.w3.org/XML/1998/namespace}lang")

    @property
    def translation_languages(self):
        """All document translation languages.
        """
        return self.xmltree.xpath(
            '//sub-article[@article-type="translation"]/@xml:lang'
        )

    @property
    def assets_names(self):
        attr_name = "{http://www.w3.org/1999/xlink}href"
        return [node.get(attr_name) for node in self.elements_which_has_xlink_href]

    @property
    def is_document_deletion(self):
        """True if delete tag is present.
        """
        return self.xmltree.find(".//article-id[@specific-use='delete']") is not None
