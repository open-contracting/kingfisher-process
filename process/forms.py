from django import forms
from django.conf import settings
from django.core.cache import cache
from django.utils.translation import gettext_lazy as _

from process.models import Collection, CollectionNote
from process.scrapy import spiders


class CollectionForm(forms.ModelForm):
    class Meta:
        model = Collection
        fields = ['source_id', 'data_version', 'sample']

    force = forms.BooleanField(required=False)

    def clean(self):
        cleaned_data = super().clean()

        if settings.SCRAPYD['url']:
            options = cache.get_or_set('scrapyd_spiders', spiders)
            for spider in options:
                options.append(spider + '_local')

            source_id = cleaned_data.get('source_id')
            force = cleaned_data.get('force')

            if source_id not in options and not force:
                # TODO: The UI (web or CLI) should error, explaining that the given source_id is not recognized, and
                # that the user can set the "force" option to ignore this error.
                pass
        else:
            # TODO: The UI (web or CLI) should warn that the source_id could not be validated, because it has no
            # connection to Scrapyd.
            pass


class CollectionNoteForm(forms.ModelForm):
    class Meta:
        model = CollectionNote
        fields = ['collection', 'note']

    @property
    def error_messages(self):
        messages = []
        for field, error_list in self.errors.as_data().items():
            for error in error_list:
                if error.code == 'required':
                    message = _('%(field)s "%(value)s" cannot be blank' % {
                        'field': field, 'value': self[field].data})
                elif error.code == 'unique_together' and error.params['unique_check'] == ('collection', 'note'):
                    message = _('Collection %(id)s already has the note "%(note)s"') % {
                        'id': self['collection'].data.pk, 'note': self['note'].data}
                elif error.params:
                    message = error.message % error.params
                else:
                    message = error.message
                messages.append(str(message))
        return '\n'.join(messages)
