
import os
import sys
import tempfile

import yaml

DEFAULT_CONFIG = 'mbeam.yaml'


SETTINGS = dict(
    CLIENT_TILE_SIZE=512,
    IMAGE_COORDINATES_FILE='image_coordinates.txt',
    METADATA_FILE='metadata.txt',
    IMAGE_PREFIX='thumbnail_',
    INVERT=True,
    CACHE_CLIENT_TILES=False,
    CLIENT_TILE_CACHE_FOLDER=os.path.join(tempfile.gettempdir(), 'mbeam'),
    DEFAULT_DATA_FOLDER='data',
    LUTS_FILE_SUFFIX=None,
)


def load_settings(config_file=DEFAULT_CONFIG):
    settings = SETTINGS.copy()
    if os.path.isfile(config_file):
        with open(config_file) as f:
            cfg = yaml.load(f.read()) or {}
        for setting in cfg:
            if setting.upper() not in SETTINGS:
                sys.stderr.write('ERROR: unsupported setting %s\n' % setting)
                sys.exit(1)
            else:
                settings[setting.upper()] = cfg[setting]
    else:
        print 'WARNING: not loading config - using default settings'
    globals().update(settings)
    return settings