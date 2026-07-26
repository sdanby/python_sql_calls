"""Compatibility wrapper for the canonical product backend.

The full mirrored Render backend implementation was retired to reduce duplication.
Use app.py as the canonical backend module.
"""

from app import app


if __name__ == '__main__':
    app.run()
