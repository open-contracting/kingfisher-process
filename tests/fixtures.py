from process.models import Collection


def collection(**kwargs):
    return Collection(
        source_id="france", data_version="2001-01-01 00:00:00", store_start_at="2001-01-01 00:00:00", **kwargs
    )


def compiled_collection(**kwargs):
    return Collection(
        source_id="france", data_version="2001-01-01 00:00:00", store_start_at="2001-01-01 00:00:00", **kwargs
    )
