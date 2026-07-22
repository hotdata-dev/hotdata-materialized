import django
from django.conf import settings


def pytest_configure():
    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=["tests"],
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": ":memory:",
                }
            },
            HOTDATA_MATERIALIZED={
                "API_KEY": "hd_test",
                "WORKSPACE_ID": "ws_test",
            },
        )
        django.setup()
