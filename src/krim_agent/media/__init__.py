"""Session-scoped authorized image viewing.

Import concrete service/delivery modules at composition time to avoid loading the
model SDK when only raster-container validation is needed.
"""
