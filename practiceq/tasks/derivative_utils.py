from PIL import Image
import logging
from tempfile import NamedTemporaryFile
from subprocess import check_call, check_output
base_url = "https://cc.lib.ou.edu"

def _formatextension(imageformat):
    """ get common file extension of image format """
    imgextensions = {"JPEG": "jpg",
                     "TIFF": "tif",
                             }
    try:
        return imgextensions[imageformat.upper()]
    except KeyError:
        return imageformat.lower()

def _params_as_string(outformat="", filter="", scale=None, crop=None):
    """
    Internal function to return image processing parameters as a single string
    Input: outformat="TIFF", filter="ANTIALIAS", scale=0.5, crop=[10, 10, 200, 200]
    Returns: tiff_050_antialias_10_10_200_200
    """
    imgformat = outformat.lower()
    imgfilter = filter.lower() if scale else None  # do not include filter if not scaled
    imgscale = str(int(scale * 100)).zfill(3) if scale else "100"
    imgcrop = "_".join((str(x) for x in crop)) if crop else None
    return "_".join((x for x in (imgformat, imgscale, imgfilter, imgcrop) if x))

# FIXME:errors out on tiff derivatives, png -> jpeg convertion.
def _processimage(inpath, outpath, outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Internal function to create image derivatives

    """
    try:
        image = Image.open(inpath)
    except (IOError, OSError):
        # workaround for Pillow not handling 16bit images
        if "16-bit" in check_output(("identify", inpath)):
            with NamedTemporaryFile() as tmpfile:
                check_call(("convert", inpath, "-depth", "8", tmpfile.name))
                image = Image.open(tmpfile.name)
        else:
            raise Exception

    if crop:
        image = image.crop(crop)

    if scale:
        try:
            imagefilter = getattr(Image, filter.upper())
        except(AttributeError):
            print("Please Provide the correct filter for Image e.g - ANTIALIAS")
        size = [x * scale for x in image.size]
        image.thumbnail(size, imagefilter)
        print((inpath,outpath,outformat,filter,scale,crop))

    try:
        image.save(outpath,outformat)
    except KeyError:
        print("Please Provide the correct OutputFormat for the image")
            



