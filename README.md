to run:     
    uvicorn main:app --reload

go to: 
    http://127.0.0.1:8000/docs
    http://127.0.0.1:8000/upload-ui

delete db:
    rm -f protege.db

deps:
    pip install -r requirements.txt

browsing endpoints:
- GET /preview?partner_id=<partner_id>&dataset_name=<dataset_name>
- GET /datasets/by-name?partner_id=<partner_id>&dataset_name=<dataset_name>
- GET /thumbnail?partner_id=<partner_id>&dataset_name=<dataset_name>&path=<relpath>



Instructions from takehome:
At Protege, we constantly ingest data from new partners where each has a variety of data types. One challenge we have faced is how do we empower our internal team and customers to not only be able to browse the data but also to understand it easily. Traditional companies use a strong ETL pipeline where the data gets heavily homogenized. Protege does not want to take this approach as data fidelity and utility is lost. We would prefer to build a platform or tool that makes any combination of data discoverable and easily digestible by our customers.



For this assignment, design (details below) a customer facing platform or tool that supports data types ranging from:






data dumps in parquet format (various folders of parquet)





You can also think of these as a bunch of SQL tables per data partner



various well named folders of well named images



where each data partner can add one or both of the above data types.



Please feel free to submit whatever documents or items you would like to present on. For example, we’ve seen combinations of system designs, architecture diagrams, and sometimes even some code/pseudo code but use your best judgement. We’re not looking for any single solution but are looking to understand your software and architecture first principles, fundamentals, and understanding in your presentation.