from unittest import mock
from unittest.mock import patch
import builtins

from practiceq.tasks.utils import get_mmsid, get_marc_datafield, get_marc_subfield_text
from nose.tools import assert_true, assert_false, assert_equal, assert_not_equal, assert_is_none, nottest, assert_raises
import yaml
from lxml import etree as ET

def test_get_mmsid():
    bag_name = "Hello_world_98745612"
    mmsid = get_mmsid(bag_name)
    assert_equal(mmsid, '98745612')
    bag_name = "helloworld"
    read_data = yaml.dump({"FIELD_EXTERNAL_DESCRIPTION": "Abbati 1703 9932140502042"})
    mock_open = mock.mock_open(read_data=read_data)
    with patch("builtins.open", mock_open):
        mmsid = get_mmsid(bag_name, 'filename')
    assert_equal(mmsid, '9932140502042')
    read_data = yaml.dump({"FIELD_EXTERNAL_DESCRIPTIO":"Abbati 1703 9932140502042"})
    mock_open = mock.mock_open(read_data=read_data)
    with patch("builtins.open",mock_open):
        mmsid = get_mmsid(bag_name,'filename')
    assert_equal(mmsid,None)
    read_data = yaml.dump({"FIELD_EXTERNAL_DESCRIPTION": "Abbati 1703 a932140502042"})
    mock_open = mock.mock_open(read_data=read_data)
    with patch("builtins.open", mock_open):
        mmsid = get_mmsid(bag_name, 'filename')
    assert_equal(mmsid, None)

def test_get_marc_datafield():
    xml = """
    <marc>
    <record>
    <datafield ind1="1" ind2="0" tag="245">
    <subfield code="a">Epitome metheorologica de' tremoti,</subfield>
    <subfield code="b">con la cronologia di tutti quelli, che sono occorsi in Roma dalla creatione del mundo sino agl' ultimi successi sotto il pontificato del regnante pontefice Clemente XI.</subfield>
    <subfield code="c">Aggiuntovi per fine un catalogo di tutti gli autori theologici, scritturali, filosofici, legali, politici, &amp; istorici sacri, e profani, che hanno discorso, e scritto de' terremoti.</subfield>
    </datafield>
    </record>
    </marc>
    """
    tag_id = "245"
    root = ET.fromstring(xml)
    val = root.xpath("record/datafield[@tag={0}]".format(tag_id))[0]
    response = get_marc_datafield(tag_id, root)
    assert_equal(val, response)
    tag_id="120"
    response=get_marc_datafield(tag_id,root)
    assert_equal(response,None)

def test_get_marc_subfield_text():
    xml = """
        <marc>
        <record>
        <datafield ind1="1" ind2="0" tag="245">
        <subfield code="a">Epitome metheorologica de' tremoti,</subfield>
        <subfield code="b">con la cronologia di tutti quelli, che sono occorsi in Roma dalla creatione del mundo sino agl' ultimi successi sotto il pontificato del regnante pontefice Clemente XI.</subfield>
        <subfield code="c">Aggiuntovi per fine un catalogo di tutti gli autori theologici, scritturali, filosofici, legali, politici, &amp; istorici sacri, e profani, che hanno discorso, e scritto de' terremoti.</subfield>
        </datafield>
        </record>
        </marc>
        """
    sub_code = 'a'
    tag_id = "245"
    root = ET.fromstring(xml)
    val= root.xpath("record/datafield[@tag={0}]/subfield[@code='{1}']".format(tag_id, sub_code))[0].text
    response=get_marc_subfield_text(tag_id,sub_code,root)
    assert_equal(response,val)
    sub_code='b'
    val = root.xpath("record/datafield[@tag={0}]/subfield[@code='{1}']".format(tag_id, sub_code))[0].text
    response = get_marc_subfield_text(tag_id, sub_code, root)
    assert_equal(response, val)
    sub_code='c'
    tag_id = "24" #to get Index error.
    root = ET.fromstring(xml)
    response = get_marc_subfield_text(tag_id, sub_code, root)
    assert_equal(response, None)

@patch("practiceq.tasks.utils.get_marc_datafield")
@patch("practiceq.tasks.utils.get_marc_subfield_text")
def test_get_title_from_marc(mock_sub_field,mock_datafield):
    xml = """
            <marc>
            <record>
            <datafield ind1="1" ind2="0" tag="245">
            <subfield code="a">Epitome metheorologica de' tremoti,</subfield>
            <subfield code="b">con la cronologia di tutti quelli, che sono occorsi in Roma dalla creatione del mundo sino agl' ultimi successi sotto il pontificato del regnante pontefice Clemente XI.</subfield>
            <subfield code="c">Aggiuntovi per fine un catalogo di tutti gli autori theologici, scritturali, filosofici, legali, politici, &amp; istorici sacri, e profani, che hanno discorso, e scritto de' terremoti.</subfield>
            </datafield>
            </record>
            </marc>
            """
    with patch.dict('tag_preferences',{'130':['a']}):
        mock_datafield.return_value=True
        mock_sub_field.return_value="Epitome metheorologica de' tremoti,"
