from unittest import TestCase, skip

from lxml import etree

from common.sps_package import (
    SPS_Package,
    extract_number_and_supplment_from_issue_element,
    parse_issue,
)



class TestParseIssue(TestCase):
    """
    Estes testes são para explicitar a saída de
    parse_issue usando o contéudo de <issue></issue>
    """
    def test_parse_issue_for_5_parenteses_suppl(self):
        expected = "05-s0"
        result = parse_issue("5 (suppl)")
        self.assertEqual(expected, result)

    def test_parse_issue_for_5_Suppl(self):
        expected = "05-s0"
        result = parse_issue("5 Suppl")
        self.assertEqual(expected, result)

    def test_parse_issue_for_5_Suppl_1(self):
        expected = "05-s01"
        result = parse_issue("5 Suppl 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_5_spe(self):
        expected = "05-spe"
        result = parse_issue("5 spe")
        self.assertEqual(expected, result)

    def test_parse_issue_for_5_suppl(self):
        expected = "05-s0"
        result = parse_issue("5 suppl")
        self.assertEqual(expected, result)

    def test_parse_issue_for_5_suppl_1(self):
        expected = "05-s01"
        result = parse_issue("5 suppl 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_5_suppl_dot_1(self):
        expected = "05-s01"
        result = parse_issue("5 suppl. 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_25_Suppl_1(self):
        expected = "25-s01"
        result = parse_issue("25 Suppl 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_2_hyphen_5_suppl_1(self):
        expected = "2-5-s01"
        result = parse_issue("2-5 suppl 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_2spe(self):
        expected = "spe"
        result = parse_issue("2spe")
        self.assertEqual(expected, result)

    def test_parse_issue_for_Spe(self):
        expected = "spe"
        result = parse_issue("Spe")
        self.assertEqual(expected, result)

    def test_parse_issue_for_Supldot_1(self):
        expected = "s01"
        result = parse_issue("Supl. 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_Suppl(self):
        expected = "s0"
        result = parse_issue("Suppl")
        self.assertEqual(expected, result)

    def test_parse_issue_for_Suppl_12(self):
        expected = "s12"
        result = parse_issue("Suppl 12")
        self.assertEqual(expected, result)

    def test_parse_issue_for_s2(self):
        expected = "s2"
        result = parse_issue("s2")
        self.assertEqual(expected, result)

    def test_parse_issue_for_spe(self):
        expected = "spe"
        result = parse_issue("spe")
        self.assertEqual(expected, result)

    def test_parse_issue_for_special(self):
        expected = "spe"
        result = parse_issue("Especial")
        self.assertEqual(expected, result)

    def test_parse_issue_for_spe_1(self):
        expected = "spe01"
        result = parse_issue("spe 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_spe_pr(self):
        expected = "spepr"
        result = parse_issue("spe pr")
        self.assertEqual(expected, result)

    def test_parse_issue_for_spe2(self):
        expected = "spe"
        result = parse_issue("spe2")
        self.assertEqual(expected, result)

    def test_parse_issue_for_spedot2(self):
        expected = "spe"
        result = parse_issue("spe.2")
        self.assertEqual(expected, result)

    def test_parse_issue_for_supp_1(self):
        expected = "s01"
        result = parse_issue("supp 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_suppl(self):
        expected = "s0"
        result = parse_issue("suppl")
        self.assertEqual(expected, result)

    def test_parse_issue_for_suppl_1(self):
        expected = "s01"
        result = parse_issue("suppl 1")
        self.assertEqual(expected, result)

    def test_parse_issue_for_suppl_12(self):
        expected = "s12"
        result = parse_issue("suppl 12")
        self.assertEqual(expected, result)

    def test_parse_issue_for_suppl_1hyphen2(self):
        expected = "s1-2"
        result = parse_issue("suppl 1-2")
        self.assertEqual(expected, result)

    def test_parse_issue_for_suppldot_1(self):
        expected = "s01"
        result = parse_issue("suppl. 1")
        self.assertEqual(expected, result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_parse_issue_for_spepr(self):
        expected = "spepr"
        result = parse_issue("spepr")
        self.assertEqual(expected, result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_parse_issue_for_supp5_1(self):
        expected = "supp5-s01"
        result = parse_issue("supp5 1")
        self.assertEqual(expected, result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_parse_issue_for_suppl_5_pr(self):
        expected = "5pr"
        result = parse_issue("suppl 5 pr")
        self.assertEqual(expected, result)


class TestSPSPackage(TestCase):
    """
    Estes testes são para explicitar a saída de
    SPS_Package.number e SPS_Package.supplement
    dado o valor de <issue></issue>
    """
    def get_sps_package(self, issue):
        xml_text = f"""
            <article><article-meta>
                <issue>{issue}</issue>
            </article-meta></article>
        """
        xmltree = etree.fromstring(xml_text)
        return SPS_Package(xmltree, "sps_package")

    def test_number_and_suppl_for_5_parenteses_suppl(self):
        expected = "5(", "0"
        _sps_package = self.get_sps_package("5 (suppl)")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_5_Suppl(self):
        expected = "5", "0"
        _sps_package = self.get_sps_package("5 Suppl")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_5_Suppl_1(self):
        expected = "5", "01"
        _sps_package = self.get_sps_package("5 Suppl 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_5_spe(self):
        expected = "5spe", None
        _sps_package = self.get_sps_package("5 spe")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_5_suppl(self):
        expected = "5", "0"
        _sps_package = self.get_sps_package("5 suppl")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_5_suppl_1(self):
        expected = "5", "01"
        _sps_package = self.get_sps_package("5 suppl 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_5_suppl_dot_1(self):
        expected = "5", "01"
        _sps_package = self.get_sps_package("5 suppl. 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_25_Suppl_1(self):
        expected = "25", "01"
        _sps_package = self.get_sps_package("25 Suppl 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_2_hyphen_5_suppl_1(self):
        expected = "2-5", "01"
        _sps_package = self.get_sps_package("2-5 suppl 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_2spe(self):
        expected = "2spe", None
        _sps_package = self.get_sps_package("2spe")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_Spe(self):
        expected = "Spe", None
        _sps_package = self.get_sps_package("Spe")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_Supldot_1(self):
        expected = None, "01"
        _sps_package = self.get_sps_package("Supl. 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_Suppl(self):
        expected = None, "0"
        _sps_package = self.get_sps_package("Suppl")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_Suppl_12(self):
        expected = None, "12"
        _sps_package = self.get_sps_package("Suppl 12")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_s2(self):
        expected = "s2", "2"
        _sps_package = self.get_sps_package("s2")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_spe(self):
        expected = "spe", None
        _sps_package = self.get_sps_package("spe")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_special(self):
        expected = "Especial", None
        _sps_package = self.get_sps_package("Especial")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_spe_1(self):
        expected = "spe1", None
        _sps_package = self.get_sps_package("spe 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_spe_pr(self):
        expected = "spepr", None
        _sps_package = self.get_sps_package("spe pr")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_spe2(self):
        expected = "spe2", None
        _sps_package = self.get_sps_package("spe2")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_spedot2(self):
        expected = "spe.2", None
        _sps_package = self.get_sps_package("spe.2")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_spepr(self):
        expected = "spepr", None
        _sps_package = self.get_sps_package("spepr")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_supp_1(self):
        expected = None, "01"
        _sps_package = self.get_sps_package("supp 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_suppl(self):
        expected = None, "0"
        _sps_package = self.get_sps_package("suppl")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_suppl_1(self):
        expected = None, "01"
        _sps_package = self.get_sps_package("suppl 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_suppl_12(self):
        expected = None, "12"
        _sps_package = self.get_sps_package("suppl 12")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_suppl_1hyphen2(self):
        expected = None, "1-2"
        _sps_package = self.get_sps_package("suppl 1-2")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    def test_number_and_suppl_for_suppldot_1(self):
        expected = None, "01"
        _sps_package = self.get_sps_package("suppl. 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_number_and_suppl_for_supp5_1(self):
        expected = "supp5", "1"
        _sps_package = self.get_sps_package("supp5 1")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_number_and_suppl_for_suppl_5_pr(self):
        expected = None, "5pr"
        _sps_package = self.get_sps_package("suppl 5 pr")
        result = _sps_package.number
        self.assertEqual(expected[0], result)
        result = _sps_package.supplement
        self.assertEqual(expected[1], result)


class TestExtractNumberAndSupplmentFromIssueElement(TestCase):
    """
    Extrai do conteúdo de <issue>xxxx</issue>, os valores number e suppl.
    Valores possíveis
    5 (suppl), 5 Suppl, 5 Suppl 1, 5 spe, 5 suppl, 5 suppl 1, 5 suppl. 1,
    25 Suppl 1, 2-5 suppl 1, 2spe, Spe, Supl. 1, Suppl, Suppl 12,
    s2, spe, spe 1, spe pr, spe2, spe.2, spepr, supp 1, supp5 1, suppl,
    suppl 1, suppl 5 pr, suppl 12, suppl 1-2, suppl. 1

    """
    def test_number_and_suppl_for_5_parenteses_suppl(self):
        expected = "5", "0"
        result = extract_number_and_supplment_from_issue_element("5 (suppl)")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_5_Suppl(self):
        expected = "5", "0"
        result = extract_number_and_supplment_from_issue_element("5 Suppl")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_5_Suppl_1(self):
        expected = "5", "1"
        result = extract_number_and_supplment_from_issue_element("5 Suppl 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_5_spe(self):
        expected = "5spe", None
        result = extract_number_and_supplment_from_issue_element("5 spe")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_5_suppl(self):
        expected = "5", "0"
        result = extract_number_and_supplment_from_issue_element("5 suppl")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_5_suppl_1(self):
        expected = "5", "1"
        result = extract_number_and_supplment_from_issue_element("5 suppl 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_5_suppl_dot_1(self):
        expected = "5", "1"
        result = extract_number_and_supplment_from_issue_element("5 suppl. 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_25_Suppl_1(self):
        expected = "25", "1"
        result = extract_number_and_supplment_from_issue_element("25 Suppl 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_2_hyphen_5_suppl_1(self):
        expected = "2-5", "1"
        result = extract_number_and_supplment_from_issue_element("2-5 suppl 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_2spe(self):
        expected = "2spe", None
        result = extract_number_and_supplment_from_issue_element("2spe")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_Spe(self):
        expected = "spe", None
        result = extract_number_and_supplment_from_issue_element("Spe")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_Supldot_1(self):
        expected = None, "1"
        result = extract_number_and_supplment_from_issue_element("Supl. 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_Suppl(self):
        expected = None, "0"
        result = extract_number_and_supplment_from_issue_element("Suppl")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_Suppl_12(self):
        expected = None, "12"
        result = extract_number_and_supplment_from_issue_element("Suppl 12")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_s2(self):
        expected = None, "2"
        result = extract_number_and_supplment_from_issue_element("s2")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_spe(self):
        expected = "spe", None
        result = extract_number_and_supplment_from_issue_element("spe")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_special(self):
        expected = "spe", None
        result = extract_number_and_supplment_from_issue_element("Especial")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_spe_1(self):
        expected = "spe1", None
        result = extract_number_and_supplment_from_issue_element("spe 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_spe_pr(self):
        expected = "spepr", None
        result = extract_number_and_supplment_from_issue_element("spe pr")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_spe2(self):
        expected = "spe2", None
        result = extract_number_and_supplment_from_issue_element("spe2")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_spedot2(self):
        expected = "spe2", None
        result = extract_number_and_supplment_from_issue_element("spe.2")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_supp_1(self):
        expected = None, "1"
        result = extract_number_and_supplment_from_issue_element("supp 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_suppl(self):
        expected = None, "0"
        result = extract_number_and_supplment_from_issue_element("suppl")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_suppl_1(self):
        expected = None, "1"
        result = extract_number_and_supplment_from_issue_element("suppl 1")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_suppl_12(self):
        expected = None, "12"
        result = extract_number_and_supplment_from_issue_element("suppl 12")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_suppl_1hyphen2(self):
        expected = None, "1-2"
        result = extract_number_and_supplment_from_issue_element("suppl 1-2")
        self.assertEqual(expected, result)

    def test_number_and_suppl_for_suppldot_1(self):
        expected = None, "1"
        result = extract_number_and_supplment_from_issue_element("suppl. 1")
        self.assertEqual(expected, result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_number_and_suppl_for_spepr(self):
        expected = "spepr", None
        result = extract_number_and_supplment_from_issue_element("spepr")
        self.assertEqual(expected, result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_number_and_suppl_for_supp5_1(self):
        expected = "supp5", "1"
        result = extract_number_and_supplment_from_issue_element("supp5 1")
        self.assertEqual(expected, result)

    @skip("Encontrado no sistema, porém fora do padrão aceitável")
    def test_number_and_suppl_for_suppl_5_pr(self):
        expected = None, "5pr"
        result = extract_number_and_supplment_from_issue_element("suppl 5 pr")
        self.assertEqual(expected, result)
