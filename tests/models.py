from django.db import models


class Event(models.Model):
    user_id = models.CharField(max_length=255)
    event_type = models.CharField(max_length=255)
    amount = models.IntegerField(null=True)
    created_at = models.DateTimeField()

    class Meta:
        app_label = "tests"
        db_table = "events"
