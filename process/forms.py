from django.forms import ModelForm
from django.utils.translation import gettext_lazy as _

from process.models import CollectionNote


class CollectionNoteForm(ModelForm):
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
                    message = _('Collection %(collection_id)s already has the note "%(note)s"') % {
                        'collection_id': self['collection'].data.pk, 'note': self['note'].data}
                elif error.params:
                    message = error.message % error.params
                else:
                    message = error.message
                messages.append(str(message))
        return '\n'.join(messages)
