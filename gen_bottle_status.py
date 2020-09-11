import requests
from dask.distributed import Client
from dask import delayed, compute
import logging

from cchdo.auth.session import session as s

from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]",
    handlers=[RichHandler()]
)

log = logging.getLogger("rich")


@delayed
def process_path(file_id, path):
    from hydro.exchange import read_exchange
    log.info(f"Processing file id {file_id}")
    try:
        return file_id, read_exchange(path)
    except ValueError as error:
        log.exception(f"File {file_id}: error")
        return file_id, error


if __name__ == "__main__":
    from hydro.exchange import Exchange
    from hydro._version import version as hydro_version
    from cchdo.params import _version as params_version
    log.info("Loading cchdo metadata")
    cruises = s.get("https://cchdo.ucsd.edu/api/v1/cruise/all").json()
    files = s.get("https://cchdo.ucsd.edu/api/v1/file/all").json()
    
    cruises = {c["id"]: c for c in cruises}
    files = {f["id"]: f for f in files}
    
    log.info("Processing Files")
    
    tasks = []
    for file_id, file_metadata in files.items():
        if (file_metadata["data_type"] != "bottle" 
                or file_metadata["data_format"] != "exchange"
                or file_metadata["role"] != "dataset"):
            continue
        path = f"https://cchdo.ucsd.edu/data/{file_id}/dummy"
        tasks.append(process_path(file_id, path))
    
    client = Client()
    results = compute(tasks)

    with open("index.html", "w") as f:
        f.write(f"""<html>
        <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
        </head>
        <body><table class="table"><thead>
        <h2>Versions</h2>
        cchdo.hydro: {hydro_version}</br>
        cchdo.params: {params_version.version}
        <h2>Files</h2>
        <tr>
        <th>Cruise(s)</th>
        <th>Start Date</th>
        <th>File ID</th>
        <th>File Download</th>
        <th>NetCDF File</th>
        </tr></thead><tbody>""")
        for result in results[0]:
            file_id, res = result
            crs = [cruises[c]["expocode"] for c in files[file_id]["cruises"]]
            date = ",".join([cruises[c]["startDate"] for c in
                files[file_id]["cruises"]])
            crs = ", ".join([f"<a href='https://cchdo.ucsd.edu/cruise/{ex}'>{ex}</a>" for ex in crs])
            fn = files[file_id]["file_name"]
            if isinstance(res, Exchange):
                log.info(f"Writing to netCDF: {res}")
                res.to_xarray().to_netcdf(f"nc/{fn}.nc")
                f.write(f"""<tr class='table-success'>
                        <td>{crs}</td>
                        <td>{date}</td>
                        <td>{file_id}</td>
                        <td><a href="https://cchdo.ucsd.edu/data/{file_id}/{fn}">{fn}</a></td>
                        <td><a href="nc/{fn}.nc">{fn}.nc</a> ({len(res)} profiles)</td>
                        </tr>""")
            else:
                f.write(f"""<tr class='table-warning'>
                <td>{crs}</td>
                <td>{date}</td>
                <td>{file_id}</td>
                <td><a href="https://cchdo.ucsd.edu/data/{file_id}/{fn}">{fn}</a></td>
                <td>Error:{repr(res)}</td>
                </tr>""")
        f.write("</tbody></table></body></html>")
    client.shutdown()
