import os
from inspect import cleandoc
from json import dumps
from unittest import mock
from unittest.mock import patch, Mock, mock_open

import bagit
import tempfile

import jinja2
from nose.tools import assert_raises, assert_equal, assert_true, assert_false
from practiceq.tasks.recipe_utils import bag_derivative, recipe_file_creation, make_recipe, process_manifest


#@patch("practiceq.tasks.recipe_utils.bagit.make_bag")
#@patch("practiceq.tasks.recipe_utils.bagit.Bag")
@patch("practiceq.tasks.recipe_utils._get_path")
def test_bag_derivative_validity(path):
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir,"x.txt"),'w') as f:
            f.write("This is a test file")
        path.return_value=tmpdir
        bag = bagit.make_bag(tmpdir)
        bag.save(manifests=True)
        bag_derivative("Abbati_1703", 'x', True)
        bag1 = bagit.Bag(tmpdir)
        assert_equal(bag1.info['External-Description'], "Abbati_1703")
        #assert_true(bag.is_valid())
    #Bag.return_value=""
    #bagit.make_bag(path)


    #Bag.return_value = ""
    #bagit.make_bag(path).save.assert_called_once()
    #Bag.return_value.save.assert_called_once()
    #assert_true(os.path.exists(tmpdir+"/data"))

    #assert_equal(bagit.)
    #path.return_value=2
    #assert_false(bagit.BagError)
@patch("practiceq.tasks.recipe_utils._get_path")
def test_bag_derivative_validity_1(path):
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir,"x.txt"),'w') as f:
            f.write("This is a test file")
        path.return_value=tmpdir
        bag_derivative("Abbati_1703", 'x', True)
        bag = bagit.Bag(tmpdir)
        assert_true(bag.is_valid())



#might need other one
@patch("practiceq.tasks.recipe_utils.make_recipe")
@patch("practiceq.tasks.recipe_utils._get_path")
def test_recipe_file_creation(path,recipe):
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir,"x.txt"),'w') as f:
            f.write("This is a test file")
        path.return_value=tmpdir
        bag =bagit.make_bag(tmpdir)
        bag.save(manifests=True)
        recipe.return_value = "Some content".encode("UTF-8")
        recipe_file_creation("Abbati", "999655522", "formatparams")
        assert_true(os.path.isfile(os.path.join(tmpdir, "Abbati.json")))
        with open(os.path.join(tmpdir, "Abbati.json"),"r") as f:
            value = f.read()
        assert_equal(value,"Some content")

@patch("practiceq.tasks.recipe_utils.process_manifest")
@patch("practiceq.tasks.recipe_utils.get_title_from_marc")
@patch("practiceq.tasks.recipe_utils.get_marc_xml")
@patch("practiceq.tasks.recipe_utils.get_bib_record")
@patch("practiceq.tasks.recipe_utils._get_path")
def test_make_recipe(path,bib_record,marc_xml,title,pages):
    test_dic = {
    "recipe": {
        "import": "book",
        "update": "false",
        "uuid": "ddc7d4d4-a136-5ac8-b4c1-4bc7bf3500a7",
        "label": "Some return value",
        "metadata": {
            "marcxml": "https://bag.ou.edu/derivative/Abbati_1703/jpeg_040_antalias/marc.xml"
        },
        "pages": [
            {
                "label": 0,
                "file": "https://bag.ou.edu/derivative/Abbati_1703/jpeg_040_antalias/data/jpeg_040_antalias/001.jpg",
                "sha256": "94020a6b2de62416631bcfa8f9ddb91638506f1b8fe5e70e36c7ddc4e4f9c73d",
                "sha512": "09eccb401716fecee5a70944961b617ea83ee942362092c4dcef86d885a84f6a021fba5ab071b0e5901cef9fc3a4432e67203336d351e19030b875e73da35f22",
                "exif": "jpeg_040_antalias.exif.txt",
                "uuid": "f92d7f24-26d3-521b-8b83-6e8fe8e1e204"
            },
            {
                "label": 1,
                "file": "https://bag.ou.edu/derivative/Abbati_1703/jpeg_040_antalias/data/jpeg_040_antalias/002.jpg",
                "sha256": "16a1033da6d6b8a5fe53fa8a1a6ac22465ce86a1790dd5e00692f8161a405853",
                "sha512": "5e44654034ea3981b9de9e977bafa459e63334e7cbbccc7273cd18d7caddf8cd378833380baa25a0b42a04969baaef21fdcd3cb2e4e209910ac93d31f4ff1337",
                "exif": "jpeg_040_antalias.exif.txt",
                "uuid": "0f8c281d-6533-5116-b010-f1ff960c621d"
            }
        ]
    }
}
    bib_record.return_value=Mock(content="some return value")
    path.return_value="/path"
    marc_xml.return_value=True
    title.return_value = "Some return value"
    pages.return_value = [{
                "label": 0,
                "file": "https://bag.ou.edu/derivative/Abbati_1703/jpeg_040_antalias/data/jpeg_040_antalias/001.jpg",
                "sha256": "94020a6b2de62416631bcfa8f9ddb91638506f1b8fe5e70e36c7ddc4e4f9c73d",
                "sha512": "09eccb401716fecee5a70944961b617ea83ee942362092c4dcef86d885a84f6a021fba5ab071b0e5901cef9fc3a4432e67203336d351e19030b875e73da35f22",
                "exif": "jpeg_040_antalias.exif.txt",
                "uuid": "f92d7f24-26d3-521b-8b83-6e8fe8e1e204"
            },
            {
                "label": 1,
                "file": "https://bag.ou.edu/derivative/Abbati_1703/jpeg_040_antalias/data/jpeg_040_antalias/002.jpg",
                "sha256": "16a1033da6d6b8a5fe53fa8a1a6ac22465ce86a1790dd5e00692f8161a405853",
                "sha512": "5e44654034ea3981b9de9e977bafa459e63334e7cbbccc7273cd18d7caddf8cd378833380baa25a0b42a04969baaef21fdcd3cb2e4e209910ac93d31f4ff1337",
                "exif": "jpeg_040_antalias.exif.txt",
                "uuid": "0f8c281d-6533-5116-b010-f1ff960c621d"
            }]
    actual_value=make_recipe("Abbati_1703",{"mmsid":"989745220100"},"/paths","jpeg_040_antalias",None)
    expected_value = dumps(test_dic, indent=4, ensure_ascii=False).encode("UTF-8")
    assert_equal(actual_value,expected_value)

def test_process_manifest():
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir,"x.txt"),'w') as f:
            f.write("This is a test file")
        bag = bagit.make_bag(tmpdir)
        payload = bag.payload_entries()
        actual_pages = process_manifest("Abbati_1703",payload,"jpeg_040_antalias")
        assert_true(len(actual_pages) == 1)


#ask about this.
def test_process_manifest_1():
    expected_pages = [
        {'label': 0,
         'file': 'https://bag.ou.edu/derivative/Abbati_1703/jpeg_040_antalias/data/x.txt',
         'sha512': 'bfe7b99fb97ba3249af08cc43d9bfd39b20c6f2e97010b5928406d1958b4701d2f053187da522c4c878dd0567e793d9ed7104905cd8df3d1f6fbac9e20c8ad91', 'sha256': 'e2d0fe1585a63ec6009c8016ff8dda8b17719a637405a4e23c0ff81339148249',
         'exif': 'x.txt.exif.txt',
         'uuid': '9fecf035-eed0-5231-908b-515e22fa2623'
         }
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir,"x.txt"),'w') as f:
            f.write("This is a test file")
        bag = bagit.make_bag(tmpdir)
        payload = bag.payload_entries()
        actual_pages = process_manifest("Abbati_1703",payload,"jpeg_040_antalias")
        assert_equal(actual_pages,expected_pages)

"""
def test_pages_list():
    actual_pages=pages_list()
    assert_true(len(actual_pages) == 1)
"""
