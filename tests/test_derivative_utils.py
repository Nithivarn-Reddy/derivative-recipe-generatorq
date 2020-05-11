import os
from io import StringIO
from unittest.mock import patch

from PIL import Image
from click import File
from nose.tools import assert_equal, assert_true
import tempfile
from practiceq.tasks.derivative_utils import _formatextension, _params_as_string, _processimage


def test_formatextension():
    value = _formatextension("JPEG")
    assert_equal(value,"jpg")
    value = _formatextension("TIFF")
    assert_equal(value, "tif")
    value = _formatextension("jpeg")
    assert_equal(value,"jpg")
    value = _formatextension("jpg")
    assert_equal(value, "jpg")

def test_params_as_string():
    value=_params_as_string(outformat="jpeg", filter="", scale=None, crop=None)
    assert_equal(value,"jpeg_100")
    value = _params_as_string(outformat="jpeg", filter="", scale=0.40, crop=None)
    assert_equal(value, "jpeg_040")
    value = _params_as_string(outformat="jpeg", filter="", scale=0.40, crop=[10,10,10,10])
    assert_equal(value, "jpeg_040_10_10_10_10")
    value = _params_as_string(outformat="jpeg", filter="xyz", scale=0.40, crop=[10, 10, 10, 10])
    assert_equal(value, "jpeg_040_xyz_10_10_10_10")

#@patch("practiceq.tasks.derivative_utils.PIL.Image")
def test_processimage():
    with tempfile.TemporaryDirectory() as tmpdir:
        image = Image.new("RGB", size=(100,100), color=(256, 0, 0))
        image.save(tmpdir+"/test.jpg","jpeg")
        _processimage(tmpdir+"/test.jpg",tmpdir+"/test.jpg",scale=0.40)
        assert_true(os.path.isfile(os.path.join(tmpdir, "test.jpg")))
        image = Image.open(tmpdir+"/test.jpg")
        assert_true(image.size == (40,40))

def test_processimage_1():
    with tempfile.TemporaryDirectory() as tmpdir:
        image = Image.new("RGB", size=(100,100), color=(256, 0, 0))
        image.save(tmpdir+"/test.tif")
        _processimage(tmpdir+"/test.tif",tmpdir+"/test.tif",scale=0.40)
        assert_true(os.path.isfile(os.path.join(tmpdir, "test.tif")))
        image = Image.open(tmpdir+"/test.tif")
        assert_true(image.size == (40,40))
