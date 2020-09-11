import requests
#from dask.distributed import Client
#from dask import delayed, compute
from concurrent.futures import ProcessPoolExecutor
import os
import logging

from rich.logging import RichHandler

from cchdo.auth.session import session as s

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]",
    handlers=[RichHandler()]
)

log = logging.getLogger("rich")


def process_path(data):
    from hydro.exchange import read_exchange
    from hydro.exchange import merge
    file_id, path = data
    log.info(f"File {file_id}: Start Processing")
    try:
        path2 = f"nc/{file_id}_ctd.nc"
        if os.path.exists(path2):
            log.info(f"File {path2} already exists, skipping...")
            return file_id, path2
        read_exchange(path, parallelize="none").to_xarray().to_netcdf(path2)
        log.info(f"File {file_id}: Done, written to {path2}")
        return file_id, path2
    except ValueError as error:
        log.exception(f"File {file_id}: Error")
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
        if (file_metadata["data_type"] != "ctd" 
                or file_metadata["data_format"] != "exchange"
                or file_metadata["role"] != "dataset"):
            continue
        path = f"https://cchdo.ucsd.edu/data/{file_id}/dummy"
        tasks.append((file_id, path))
        #if len(tasks) > 10:
        #    break

    log.info(f"Processing {len(tasks)} files")
    
    #client = Client(n_workers=8)
    #results = compute(tasks)
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_path, tasks)


        with open("index_ctd.html", "w") as f:
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
            <th>File ID</th>
            <th>File Download</th>
            <th>NetCDF File</th>
            </tr></thead><tbody>""")
            for result in results:
                file_id, res = result
                try:
                    crs = [cruises[c]["expocode"] for c in files[file_id]["cruises"]]
                    crs = ", ".join([f"<a href='https://cchdo.ucsd.edu/cruise/{ex}'>{ex}</a>" for ex in crs])
                except KeyError:
                    continue
                fn = files[file_id]["file_name"]
                if isinstance(res, str):
                    f.write(f"""<tr class='table-success'>
                            <td>{crs}</td>
                            <td>{file_id}</td>
                            <td><a href="https://cchdo.ucsd.edu/data/{file_id}/{fn}">{fn}</a></td>
                            <td><a href="{res}">{res}</a></td>
                            </tr>""")
                else:
                    f.write(f"""<tr class='table-warning'>
                    <td>{crs}</td>
                    <td>{file_id}</td>
                    <td><a href="https://cchdo.ucsd.edu/data/{file_id}/{fn}">{fn}</a></td>
                    <td>Error:{repr(res)}</td>
                    </tr>""")
            f.write("</tbody></table></body></html>")
    #client.shutdown()
