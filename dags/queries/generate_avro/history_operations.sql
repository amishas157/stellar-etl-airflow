export data
  options (
    uri = '{uri}',
    format = 'avro',
    overwrite=true
    )
as (
  select
    *
    except(details, details_json, batch_id, batch_insert_ts, batch_run_date),
    details.*
    except(claimants, type),
    details.type as details_type
  from {project_id}.{dataset_id}.history_operations
  where true
    and closed_at >= '{batch_run_date}'
    and closed_at < '{next_batch_run_date}'
  order by closed_at asc
)
