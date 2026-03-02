import os
import logging
import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S#_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

if not S3_BUCKET:
	raise ValueError("S3_BUCKET Environment variable is not set")

s3_client = boto3.client("s3", region_name=AWS_REGION)


class S3ServiceException(Exception):
	"""custom exception for s3 service failures"""
	pass


def generate_presigned_upload_url(s3_key: str, content_type: str, expiration: int == 600) -> str:
	"""
	Generate a pre-signed URL for uploading image to s3
	:param s3_key: Path inside s3 bucket
	:type s3_key: str
	:param content_type: MIME type of image
	:type content_type: str
	:param expiration: URL expiration time in seconds (default 10 minutes)
	:type expiration: int
	:return: Pre-Signed URL String
	:rtype: str
	"""
	try:
		url = s3_client.generate_presigned_url(
			ClientMethod="put_object",
			Params={"Bucket": S3_BUCKET, "key": s3_key, "ContentType": content_type},
			ExpiresIn=expiration
		)
		logger.info(f"Generated s3 Upload URL for key: {s3_key}")
		return url
	except ClientError as error:
		logger.error(f"could not generate the presinged url due to {error}")
		raise S3ServiceException("could not generate pre-singed url") from error


def generate_presigned_download_url(s3_key: str, expiration: int = 600):
	try:
		url = s3_client.generate_presigned_url(
			ClientMethod="get_object",
			params={"Bucket": S3_BUCKET, "key": s3_key},
			ExpiresIn=expiration
		)
		logger.info(f"Generated Pre-signed URL for keys {s3_key}")
		return url
	except ClientError as error:
		logger.error(f"Could not generate the presinged url due to {error}")
		raise S3ServiceException("Could not Generate Pre-singed URL") from error


def delete_image(s3_key: str):
	"""
	Delete the image from S3 bucket.
	:param s3_key: Path/key of the s3 image.
	:type s3_key:
	:return: None
	:rtype:
	"""
	try:
		s3_client.delete_object(
			Bucket=S3_BUCKET,
			key=s3_key
		)
	except ClientError as error:
		logger.error(f"Could not delete the error due to : {error}")
		raise S3ServiceException("Could not Delete the image") from error


def object_exists(s3_key: str) -> bool:
	"""
	Check if an object exists in S3.
	:param s3_key: Path/key inside the bucket
	:type s3_key: 
	:return: True/False Based on the existence
	:rtype: 
	"""
	try:
		s3_client.head_object(
			Bucket=S3_BUCKET,
			key=s3_key
		)
		return True
	except ClientError as error:
		if e.response["Error"]["Code"] == "404":
			return False
		logger.error(f"Error checking object existence: {str(error)}")
		raise S3ServiceException("Could not verify object existence") from error