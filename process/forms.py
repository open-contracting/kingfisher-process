from difflib import get_close_matches

from django import forms
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

import process.scrapyd
from process.models import Collection, CollectionFile, CollectionNote


class KingfisherForm(forms.ModelForm):
    @property
    def error_messages(self):
        messages = []
        for field, error_list in self.errors.as_data().items():
            for error in error_list:
                messages.append(str(self.error_message_formatter(field, error)))
        return "\n".join(messages)

    def error_message_formatter(self, field, error):
        if error.code == "required":
            return _("%(field)s %(value)r cannot be blank") % {"field": field, "value": self[field].data}
        if error.params:
            message = error.message % error.params
        else:
            message = error.message
        return f"{field}: {message}"


class CollectionForm(KingfisherForm):
    class Meta:
        model = Collection
        fields = ["source_id", "data_version", "sample", "transform_type", "parent", "steps"]

    force = forms.BooleanField(required=False)

    def clean(self):
        cleaned_data = super().clean()

        if process.scrapyd.configured():
            options = cache.get_or_set("scrapyd_spiders", process.scrapyd.spiders)
            for spider in list(options):
                options.append(spider + "_local")

            source_id = cleaned_data.get("source_id")
            force = cleaned_data.get("force")

            if source_id not in options and not force:
                params = {"value": source_id, "project": settings.SCRAPYD["project"]}
                match = get_close_matches(source_id, options, n=1)
                if match:
                    params["match"] = match[0]
                    message = _(
                        "%(value)r is not a spider in the %(project)s project of Scrapyd. Did you mean: " "%(match)s"
                    )
                else:
                    message = _("%(value)r is not a spider in the %(project)s project of Scrapyd (can be forced)")
                self.add_error("source_id", ValidationError(message, params=params, code="invalid_choice"))

    def error_message_formatter(self, field, error):
        if field == "data_version" and error.code == "invalid":
            return _('%(field)s %(value)r is not in "YYYY-MM-DD HH:MM:SS" format or is an invalid date/time') % {
                "field": field,
                "value": self[field].data,
            }
        return super().error_message_formatter(field, error)


class CollectionNoteForm(KingfisherForm):
    class Meta:
        model = CollectionNote
        fields = ["collection", "note", "code"]

    def error_message_formatter(self, field, error):
        if error.code == "unique_together" and error.params["unique_check"] == ("collection", "note"):
            return _("Collection %(id)s already has the note %(value)r") % {
                "id": self["collection"].data.pk,
                "value": self["note"].data,
            }
        return super().error_message_formatter(field, error)


class CollectionFileForm(KingfisherForm):
    class Meta:
        model = CollectionFile
        fields = ["collection", "filename", "url", "warnings", "errors"]

    def error_message_formatter(self, field, error):
        if error.code == "unique_together" and error.params["unique_check"] == ("collection", "filename"):
            return _("Collection %(id)s already contains file %(value)r") % {
                "id": self["collection"].data.pk,
                "value": self["filename"].data,
            }
        return super().error_message_formatter(field, error)
