"""The cloud API's client: Databento-style queries over your private archive.

    client = briskapi.Historical()                        # your archive identity is the key
    data = client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="trades",
                                       start="2026-10-13", end="2026-10-17")
    data.to_df()

Names and parameters follow databento.Historical. Datasets: XTKS.BRISK (your live recordings, whichever
broker's BRiSK they came through) and XTKS.BRISKDEMO (the public demo, open to every identity). Schemas:
trades, mbp-10, definition, imbalance, brisk-quotes, and OHLCV bars (ohlcv-1s/1m/1h/1d), which come only
from the cloud: built ahead from your own live recordings, for the securities and times they cover. Times
without a zone, and dates, are JST (the trading day for a date); `end` is exclusive.
See docs/cloud-api.md.
"""
from __future__ import annotations

import base64
import decimal
import io
import json
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request
import warnings

import pyarrow.parquet as pq

from briskapi import cli, identity
from briskapi._recording import BriskError


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None   # a 303 to a result is followed by hand, without the key


# Columns that hold prices: Databento's `price_type` applies to these.
_PRICES = {'price', 'open', 'high', 'low', 'close', 'ref_price', 'ind_match_price', 'upper_collar', 'lower_collar',
           'min_price_increment', 'high_limit_price', 'low_limit_price', 'base_price'}


def _is_price(name):
    return name in _PRICES or '_px_' in name or name.endswith('_price')


class DataStore:
    """A query's records, held as Parquet, with databento.DBNStore's conversions: to_df(), to_ndarray(), to_csv(),
    to_json(), to_parquet(), to_file(), replay() and iteration."""

    def __init__(self, data: bytes, schema: str):
        self._data, self.schema = data, schema

    def _arrow(self):
        return pq.read_table(io.BytesIO(self._data))

    def _frame(self, price_type='float', pretty_ts=True, map_symbols=True, tz='UTC'):
        try:
            import pandas  # noqa: F401 - Arrow's to_pandas needs it
        except ImportError as e:
            raise ImportError("Install pandas (pip install 'briskapi[pandas]') for to_df(), to_ndarray(), to_csv() "
                              'and to_json(); to_parquet(), to_file(), replay() and iteration work without it') from e
        frame = self._arrow().to_pandas()
        if not map_symbols and 'symbol' in frame.columns:
            frame = frame.drop(columns='symbol')
        for name in frame.columns:
            if _is_price(name) and frame[name].dtype.kind == 'f':
                if price_type == 'fixed':
                    frame[name] = (frame[name] * 1e9).round().astype('Int64')
                elif price_type == 'decimal':
                    frame[name] = [None if v != v else decimal.Decimal(repr(v)) for v in frame[name]]
                elif price_type != 'float':
                    raise ValueError("price_type is 'float', 'fixed' or 'decimal'")
            elif str(frame[name].dtype).startswith('datetime64'):
                if not pretty_ts:
                    frame[name] = frame[name].astype('int64')
                elif tz not in (None, 'UTC'):
                    frame[name] = frame[name].dt.tz_convert(tz)
        return frame

    def to_df(self, price_type='float', pretty_ts=True, map_symbols=True, tz='UTC', count=None):
        """A pandas DataFrame indexed by ts_recv (or ts_event where a schema has no receipt time); with `count`, an
        iterator of DataFrames of at most that many rows. `price_type`: 'float', 'fixed' (integers of 1e-9) or
        'decimal'; `tz` converts the timestamps (pretty_ts=False gives UTC nanoseconds)."""
        if not pretty_ts and tz not in (None, 'UTC'):
            raise ValueError('tz needs pretty_ts=True')
        frame = self._frame(price_type, pretty_ts, map_symbols, tz)
        index = 'ts_recv' if 'ts_recv' in frame.columns else 'ts_event'
        frame = frame.set_index(index) if index in frame.columns else frame
        if count is None:
            return frame
        return (frame.iloc[at:at + count] for at in range(0, len(frame), count))

    def to_ndarray(self, count=None):
        """The records as a numpy structured array (or, with `count`, an iterator of arrays)."""
        records = self._frame().to_records(index=False)
        if count is None:
            return records
        return (records[at:at + count] for at in range(0, len(records), count))

    def to_csv(self, path, pretty_px=True, pretty_ts=True, map_symbols=True):
        frame = self._frame('float' if pretty_px else 'fixed', pretty_ts, map_symbols)
        frame.to_csv(path, index=False)
        return Path(path)

    def to_json(self, path, pretty_px=True, pretty_ts=True, map_symbols=True):
        """JSON lines, one record each."""
        frame = self._frame('float' if pretty_px else 'fixed', pretty_ts, map_symbols)
        frame.to_json(path, orient='records', lines=True, date_format='iso', date_unit='ns')
        return Path(path)

    def to_parquet(self, path, price_type='float', pretty_ts=True, map_symbols=True):
        if (price_type, pretty_ts, map_symbols) == ('float', True, True):
            Path(path).write_bytes(self._data)
        else:
            self._frame(price_type, pretty_ts, map_symbols).to_parquet(path, index=False)
        return Path(path)

    def to_file(self, path):
        """The records as received (Parquet; Databento's would be DBN)."""
        Path(path).write_bytes(self._data)
        return Path(path)

    def __iter__(self):
        """Each record as a dict."""
        for batch in self._arrow().to_batches():
            yield from batch.to_pylist()

    def replay(self, callback):
        """Call `callback(record)` for each record, in time order."""
        for record in self:
            callback(record)

    def __len__(self):
        return pq.ParquetFile(io.BytesIO(self._data)).metadata.num_rows

    def __repr__(self):
        return f'DataStore(schema={self.schema!r}, records={len(self)})'


class Historical:
    """Databento-style historical queries over your private archive (the cloud API)."""

    def __init__(self, key: str | None = None, gateway: str | None = None):
        if key is None:
            key, created = identity.ensure()
            if created:
                warnings.warn(f'Made your archive identity (owner {cli.owner(key)}); it is your API key. Keep its recovery '
                              'code: `brisk identity --recovery-code`', stacklevel=2)
        self.key = key
        self.gateway = (gateway or cli.settings().get('query_url') or '').rstrip('/')
        if not self.gateway:
            raise BriskError('This archive has no query API (archive.json has no query_url)')
        cli.secure(self.gateway)   # the key goes with every request
        self.timeseries = _Timeseries(self)
        self.metadata = _Metadata(self)

    def _call(self, endpoint, params, post=False) -> tuple[bytes, str]:
        params = {k: (','.join(map(str, v)) if isinstance(v, (list, tuple, set)) else str(v).lower() if isinstance(v, bool) else str(v))
                  for k, v in params.items() if v is not None}
        url = f'{self.gateway}/v0/{endpoint}'
        data = urllib.parse.urlencode(params).encode() if post else None
        if not post and params:
            url += '?' + urllib.parse.urlencode(params)
        auth = base64.b64encode(f'{self.key}:'.encode()).decode()
        request = urllib.request.Request(url, data=data, headers={'Authorization': f'Basic {auth}'})
        try:
            with urllib.request.build_opener(_NoRedirect).open(request, timeout=300) as response:
                return response.read(), response.headers.get('content-type', '')
        except urllib.error.HTTPError as error:
            if error.code in (301, 302, 303, 307) and error.headers.get('location', '').startswith('https://'):
                with urllib.request.urlopen(error.headers['location'], timeout=300) as response:
                    return response.read(), response.headers.get('content-type', '')
            try:
                detail = json.loads(error.read()).get('detail', '')
            except ValueError:
                detail = ''
            raise BriskError(f'{endpoint}: HTTP {error.code}: {detail or error.reason}') from None

    def _json(self, endpoint, params=None, post=False):
        return json.loads(self._call(endpoint, params or {}, post)[0])


class _Timeseries:
    def __init__(self, client):
        self._client = client

    def get_range(self, dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', stype_in='raw_symbol',
                  stype_out='instrument_id', limit=None, path=None) -> DataStore:
        """Records of `schema` for `symbols` (JPX codes) in [start, end), merged across your recordings and
        time-ordered. `path` also writes them there (Parquet). Records carry both `instrument_id` and the code
        (`symbol`), so `stype_out` is `instrument_id` as Databento's default."""
        if stype_out != 'instrument_id':
            raise ValueError("stype_out is 'instrument_id'; each record also has its code in `symbol`")
        data, _ = self._client._call('timeseries.get_range', dict(dataset=dataset, start=start, end=end, symbols=symbols,
                                                                  schema=schema, stype_in=stype_in, limit=limit,
                                                                  encoding='parquet'), post=True)
        store = DataStore(data, schema)
        if path:
            store.to_parquet(path)
        return store


class _Metadata:
    def __init__(self, client):
        self._client = client

    def list_datasets(self) -> list:
        return self._client._json('metadata.list_datasets')

    def list_schemas(self, dataset) -> list:
        return self._client._json('metadata.list_schemas', {'dataset': dataset})

    def list_fields(self, schema) -> list:
        return self._client._json('metadata.list_fields', {'schema': schema})

    def list_publishers(self) -> list:
        return self._client._json('metadata.list_publishers')

    def get_dataset_range(self, dataset) -> dict:
        return self._client._json('metadata.get_dataset_range', {'dataset': dataset})

    def get_dataset_condition(self, dataset, start_date=None, end_date=None) -> list:
        """Each date's condition (available or missing) and the clock windows your recordings cover (JST)."""
        return self._client._json('metadata.get_dataset_condition',
                                  {'dataset': dataset, 'start_date': start_date, 'end_date': end_date})

    def get_record_count(self, dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None) -> int:
        return self._client._json('metadata.get_record_count', dict(dataset=dataset, start=start, end=end, symbols=symbols,
                                                                    schema=schema, limit=limit), post=True)

    def get_cost(self, dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades') -> float:
        """Always 0: the cloud API is free, with quotas."""
        return self._client._json('metadata.get_cost', dict(dataset=dataset, start=start, end=end, symbols=symbols,
                                                            schema=schema), post=True)
