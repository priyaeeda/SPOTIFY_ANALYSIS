import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node tracks
tracks_node1729427678228 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-poc-4444/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1729427678228")

# Script generated for node album
album_node1729427677012 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-poc-4444/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1729427677012")

# Script generated for node artist
artist_node1729427677595 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-poc-4444/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1729427677595")

# Script generated for node Join artist and album
Joinartistandalbum_node1729427867062 = Join.apply(frame1=artist_node1729427677595, frame2=album_node1729427677012, keys1=["id"], keys2=["artist_id"], transformation_ctx="Joinartistandalbum_node1729427867062")

# Script generated for node Join with track
Joinwithtrack_node1729428000313 = Join.apply(frame1=tracks_node1729427678228, frame2=Joinartistandalbum_node1729427867062, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtrack_node1729428000313")

# Script generated for node Drop Fields
DropFields_node1729428158245 = DropFields.apply(frame=Joinwithtrack_node1729428000313, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1729428158245")

# Script generated for node destination
destination_node1729428173495 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1729428158245, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-poc-4444/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="destination_node1729428173495")

job.commit()