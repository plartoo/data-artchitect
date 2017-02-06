Python scripts to automate data processing using Vertica, S3 and Datamart.

Setup instructions:

1. Download Python (tested with 3.6)
2. (For Windows) Install Python to a folder without spaces in the path like this:
'C:\\Users\\janes.bond\\AppData\\Local\\Programs\\Python\\Python36-32'
3. `pip install pip --upgrade`
4. `pip install awscli`
5. `pip install pandas`
6. `pip install boto3`
7. `pip install vertica_python`
8. `pip install schedule` (only if you need scheduling like cron, say, bi-weekly
9. Configure AWS CLI by following this guide:
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html

In particular, type this in command prompt:

`$ aws configure`

then enter info on prompt (example below)
AWS Access Key ID [None]: BLAHACCESSKEYID
AWS Secret Access Key [None]: BLAHSECRETEACCESSKEY
Default region name [None]: us-east-1
