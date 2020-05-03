import os
from unittest import mock
from unittest.mock import patch, Mock, mock_open

import bagit
import tempfile
from nose.tools import assert_raises, assert_equal, assert_true, assert_false
from practiceq.tasks.recipe_utils import bag_derivative, recipe_file_creation


@patch("practiceq.tasks.recipe_utils.bagit.make_bag")
@patch("practiceq.tasks.recipe_utils.bagit.Bag")
@patch("practiceq.tasks.recipe_utils._get_path")
def test_bag_derivative(path,Bag,make_bag):

    path.return_value="/fakepath"
    #Bag.return_value=""
    bag_derivative("Abbati_1703",'x',True)
    #Bag.return_value = ""
    #bagit.make_bag(path).save.assert_called_once()
    Bag.return_value.save.assert_called_once()
    #assert_true(os.path.exists(tmpdir+"/data"))
    #assert_equal(Bag.return_value['External-Description'],"Abbati_1703")
    #path.return_value=2
    #assert_false(bagit.BagError)
"""
@patch("practiceq.tasks.recipe_utils.make_recipe")
@patch("practiceq.tasks.recipe_utils.bagit.Bag")
@patch("practiceq.tasks.recipe_utils._get_path")
#@patch("practiceq.tasks.recipe_utils.bagit.bag.payload_entries")
def test_recipe_file_creation(path,bag,recipe):
    path.return_value="/fakepath"
    bag.return_value=bag
    recipe.return_value=Mock(content="some string value")
    recipefile ="/fakepath/Abbati.json"
    with patch('builtins.open',mock_open()) as f:
        recipe_file_creation("Abbati", "999655522", "formatparams")
        #.write(recipe_file,"some string value".decode("UTF-8"))
        #f.assert_called_once_with(recipefile,"some string value")
        f().write.assert_called_once_with("some string value")
"""


