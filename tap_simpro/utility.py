import os
import json
import hashlib
import asyncio
import singer
from singer import metadata
import singer.metrics as metrics
from datetime import datetime

from tap_simpro.config import (
    streams,
    streams_with_details,
    streams_specify_columns,
    streams_add_specified_columns,
    streams_exclude_specified_columns,
    streams_disable_filtering,
)


# constants
# note this is going to be updated from __init__
base_url = None
strip_href_url = "/api/v1.0/companies/0/"

sub_streams = set([x for v in streams.values() for x in v])

# no rate limiting or concurrent request limit mentioned in docs https://developer.simprogroup.com/apidoc/
sem = asyncio.Semaphore(32)


def set_base_url(base):
    global base_url
    base_url = base + "/api/v1.0/companies/0"


def get_endpoint(resource):
    return {
        "accounts": "setup/accounts/chartOfAccounts",
        "activities": "setup/activities",
        "cost_centers": "setup/accounts/costCenters",
        "payable_invoices": "accounts/payable/invoices",
        "project_status_codes": "setup/statusCodes/projects",
        "schedule_rates": "setup/labor/scheduleRates",
    }.get(resource, to_camel_case(resource))


async def get_resource(
    session, resource, bookmark, schema, get_details_url=None, endpoint_override=None
):
    page_size = 250
    schema_fields = schema["properties"].keys()
    disable_filtering = resource in streams_disable_filtering

    specify_columns = resource in streams_specify_columns
    if not specify_columns:
        columns_query_string = ""
    else:
        to_exclude = streams_exclude_specified_columns.get(resource, [])
        columns_excluding_specified = [f for f in schema_fields if f not in to_exclude]
        columns_query_string = f'&columns={",".join(columns_excluding_specified) + streams_add_specified_columns.get(resource, "")}'
    # print(columns_query_string)

    async def _get(archived):
        page = 1
        while True:
            endpoint = (
                endpoint_override if endpoint_override else get_endpoint(resource)
            )
            # recurring invoices uses Removed instead of Archived
            # API ignores fields that aren't present, so can safely send both archived and removed each time
            url = f"{endpoint}/?pageSize={page_size}&page={page}&Archived={archived}&Removed={archived}&orderby=-DateModified{columns_query_string}"

            # print("URL", url)
            json = await get_basic(session, resource, url)
            # print(json)

            if len(json) == 0:
                return
            page += 1

            def _get_details_url(row):
                # use get_details_url lambda if provided, otherwise _href property if available, or use the default of resource plus ID
                return (
                    get_details_url(row)
                    if get_details_url
                    else f"{endpoint}/{row['ID']}"
                    if "_href" not in row
                    else (row["_href"].replace(strip_href_url, ""))
                )

            # if columns are specified then don't need to fetch details
            has_details = (
                streams_with_details.get(resource, True) and not specify_columns
            )
            if has_details:
                details_ls = await await_futures(
                    [
                        get_basic(session, resource, _get_details_url(row))
                        for row in json
                    ]
                )

                for d in details_ls:
                    # note that simple string comparison sorting works here, thanks to the date formatting
                    if (
                        bookmark
                        and not disable_filtering
                        and "DateModified" in d
                        and d["DateModified"] < bookmark
                    ):
                        return

                    yield d
            else:
                # if the list returns DateModified too, then use that to return early
                last_modified = json[-1].get("DateModified")
                if (
                    bookmark
                    and not disable_filtering
                    and last_modified
                    and last_modified < bookmark
                ):
                    # only add rows updated since the bookmark
                    for r in json:
                        if r.get("DateModified") >= bookmark:
                            yield r

                    return
                else:
                    for r in json:
                        yield r

            # otherwise will always finish with a guaranteed-empty request that will return []
            if len(json) < page_size:
                return

    # no query string option to get archived and unarchived (or removed and not removed), so run it once with each
    async for row in _get(False):
        yield row

    # only run a second time if records can be archived/removed, or it'll just ignore the query parameter and fetch all records a second time
    if "Archived" in schema_fields or "Removed" in schema_fields:
        async for row in _get(True):
            yield row


async def get_basic(session, resource, url):
    async with sem:
        with metrics.http_request_timer(resource) as timer:
            async with await session.get(f"{base_url}/{url}") as resp:
                timer.tags[metrics.Tag.http_status_code] = resp.status
                resp.raise_for_status()
                return await resp.json()


def transform_record(record, properties, json_encoded_columns):
    if "CustomFields" in record:
        map = {}
        for field in record["CustomFields"]:
            map[field["CustomField"]["Name"]] = field["Value"]
        record["CustomFields"] = map

    for col in json_encoded_columns:
        record[col] = json.dumps(record[col])

    return record


async def await_futures(futures):
    return await asyncio.gather(*futures)


date_format = "%Y-%m-%d"
datetime_format = "%Y-%m-%d %H:%M:%S"


def format_date(dt, format=datetime_format):
    return datetime.strftime(dt, format)


def parse_date(dt, format=date_format):
    return datetime.strptime(dt, format)


def try_parse_date(s, parse_format=date_format):
    try:
        return format_date(parse_date(s, parse_format), date_format)
    except:
        return None


def hash(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


# per https://stackoverflow.com/questions/19053707/converting-snake-case-to-lower-camel-case-lowercamelcase#19053800
def to_camel_case(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])
