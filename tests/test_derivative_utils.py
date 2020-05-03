from unittest.mock import patch

from PIL.Image import Image
from nose.tools import assert_equal
import tempfile
from practiceq.tasks.derivative_utils import _formatextension, _params_as_string


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


@patch("practiceq.tasks.derivative_utils.PIL.Image")
def test_processimage(Image):
    image = tempfile.NamedTemporaryFile(suffix=".jpg").name
    image = Image.crop(image)
    with scale=True:
        imagefilter = getattr(image,"ANTIALIAS")
        Image.thumbnail.assert_called_once()