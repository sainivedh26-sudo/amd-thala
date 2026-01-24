"""
AWS Attachment Processor
Handles S3 upload and Textract extraction for Slack and Jira attachments
"""
import os
import logging
import boto3
import tempfile
import requests
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from PIL import Image
import io

load_dotenv()

logger = logging.getLogger(__name__)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')  # Optional, for temporary credentials
AWS_REGION = os.getenv('AWS_REGION', 'us-east-2')
S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'thala-images')

# Supported image formats for Textract
SUPPORTED_IMAGE_FORMATS = ['.jpg', '.jpeg', '.png', '.pdf', '.gif', '.bmp', '.tiff', '.webp']

class AWSAttachmentProcessor:
    """
    Processes attachments from Slack/Jira:
    1. Downloads attachment
    2. Uploads to S3
    3. Extracts text using Textract
    4. Returns extracted text to be added to incident context
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize S3 client
        try:
            s3_kwargs = {
                'aws_access_key_id': AWS_ACCESS_KEY_ID,
                'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
                'region_name': AWS_REGION
            }
            if AWS_SESSION_TOKEN:
                s3_kwargs['aws_session_token'] = AWS_SESSION_TOKEN
            
            self.s3_client = boto3.client('s3', **s3_kwargs)
            self.logger.info("S3 client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            self.s3_client = None
        
        # Initialize Textract client
        try:
            textract_kwargs = {
                'aws_access_key_id': AWS_ACCESS_KEY_ID,
                'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
                'region_name': AWS_REGION
            }
            if AWS_SESSION_TOKEN:
                textract_kwargs['aws_session_token'] = AWS_SESSION_TOKEN
            
            self.textract_client = boto3.client('textract', **textract_kwargs)
            self.logger.info("Textract client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Textract client: {e}")
            self.textract_client = None
    
    def _is_image_file(self, filename):
        """Check if file is a supported image format"""
        if not filename:
            return False
        ext = os.path.splitext(filename.lower())[1]
        return ext in SUPPORTED_IMAGE_FORMATS
    
    def _generate_s3_key(self, source, file_id, filename):
        """Generate S3 key for uploaded file"""
        timestamp = datetime.utcnow().strftime('%Y/%m/%d')
        # Sanitize filename
        safe_filename = "".join(c for c in filename if c.isalnum() or c in "._-")[:50]
        return f"{source}/{timestamp}/{file_id}_{safe_filename}"
    
    def upload_to_s3(self, file_path, s3_key):
        """
        Upload file to S3 bucket
        
        Args:
            file_path: Local path to file
            s3_key: S3 object key (path in bucket)
            
        Returns:
            str: S3 URL if successful, None otherwise
        """
        if not self.s3_client:
            self.logger.error("S3 client not initialized")
            return None
        
        try:
            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=S3_BUCKET,
                Key=s3_key,
                ExtraArgs={'ACL': 'public-read'}  # Make it publicly accessible
            )
            
            s3_url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
            self.logger.info(f"[S3] Uploaded file to: {s3_url}")
            return s3_url
            
        except Exception as e:
            self.logger.error(f"Error uploading to S3: {e}")
            return None
    
    def extract_text_from_local_file(self, file_path):
        """
        Extract text from local image file using Textract
        Tries detect_document_text first, falls back to analyze_document if needed
        
        Args:
            file_path: Path to local image file
            
        Returns:
            str: Extracted text, or None if error
        """
        if not self.textract_client:
            self.logger.error("Textract client not initialized")
            return None
        
        if not self._is_image_file(file_path):
            self.logger.warning(f"File format not supported for Textract: {file_path}")
            return None
        
        # Validate file exists and is readable
        if not os.path.exists(file_path):
            self.logger.error(f"File does not exist: {file_path}")
            return None
        
        try:
            with open(file_path, 'rb') as image_file:
                image_bytes = image_file.read()
            
            # Validate file is not empty
            if len(image_bytes) == 0:
                self.logger.warning(f"File is empty: {file_path}")
                return None
            
            # Validate file size (Textract has limits: 5MB for synchronous, 500MB for async)
            max_size = 5 * 1024 * 1024  # 5MB for detect_document_text
            if len(image_bytes) > max_size:
                self.logger.warning(f"File too large for Textract ({len(image_bytes)} bytes > {max_size}): {file_path}")
                return None
            
            # Validate file headers first
            is_png = len(image_bytes) >= 8 and image_bytes[:8] == b'\x89PNG\r\n\x1a\n'
            is_jpeg = len(image_bytes) >= 2 and image_bytes[:2] == b'\xff\xd8'
            
            if not (is_png or is_jpeg):
                self.logger.warning(f"[ATTACHMENT] File doesn't appear to be valid PNG/JPEG (invalid headers), skipping extraction")
                return None
            
            # Try to validate and convert image format if needed
            # Textract sometimes fails with certain PNG encodings, so we'll convert to JPEG
            try:
                img_buffer = io.BytesIO(image_bytes)
                img_buffer.seek(0)
                img = Image.open(img_buffer)
                
                # Verify the image
                img.verify()
                img_buffer.seek(0)
                img = Image.open(img_buffer)
                
                # Convert RGBA/transparent PNG to RGB JPEG for better Textract compatibility
                if img.format == 'PNG' and img.mode in ('RGBA', 'LA', 'P'):
                    self.logger.info(f"[ATTACHMENT] Converting {img.mode} PNG to RGB JPEG for Textract compatibility...")
                    # Convert to RGB
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'RGBA':
                        rgb_img.paste(img, mask=img.split()[3])  # Use alpha channel as mask
                    else:
                        rgb_img.paste(img)
                    # Convert to bytes as JPEG
                    jpeg_buffer = io.BytesIO()
                    rgb_img.save(jpeg_buffer, format='JPEG', quality=95)
                    image_bytes = jpeg_buffer.getvalue()
                    self.logger.info(f"[ATTACHMENT] Converted PNG to JPEG ({len(image_bytes)} bytes)")
                elif img.format not in ('JPEG', 'PNG', 'PDF'):
                    # Try converting other formats to JPEG
                    self.logger.info(f"[ATTACHMENT] Converting {img.format} to JPEG for Textract...")
                    if img.mode != 'RGB':
                        img = img.convert('RGB')
                    jpeg_buffer = io.BytesIO()
                    img.save(jpeg_buffer, format='JPEG', quality=95)
                    image_bytes = jpeg_buffer.getvalue()
                    self.logger.info(f"[ATTACHMENT] Converted to JPEG ({len(image_bytes)} bytes)")
            except Exception as img_error:
                self.logger.warning(f"[ATTACHMENT] Could not process image format: {img_error}, trying original bytes...")
                # Continue with original bytes if conversion fails
            
            # First try detect_document_text (simpler, faster, cheaper)
            try:
                response = self.textract_client.detect_document_text(
                    Document={'Bytes': image_bytes}
                )
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'UnsupportedDocumentException':
                    self.logger.warning(f"[TEXTRACT] detect_document_text unsupported format for {file_path}, trying analyze_document...")
                    # Try analyze_document as fallback (supports more formats)
                    try:
                        response = self.textract_client.analyze_document(
                            Document={'Bytes': image_bytes},
                            FeatureTypes=['TABLES', 'FORMS']
                        )
                    except ClientError as e2:
                        self.logger.error(f"[TEXTRACT] analyze_document also failed: {e2}")
                        return None
                else:
                    raise
            
            # Extract text from blocks
            extracted_text = []
            for block in response.get('Blocks', []):
                if block.get('BlockType') == 'LINE':
                    text = block.get('Text', '')
                    if text:
                        extracted_text.append(text)
            
            result = '\n'.join(extracted_text)
            if result.strip():
                self.logger.info(f"[TEXTRACT] Extracted {len(extracted_text)} lines from {file_path}")
                return result
            else:
                self.logger.warning(f"[TEXTRACT] No text found in image: {file_path}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error extracting text with Textract: {e}")
            return None
    
    def extract_text_from_s3(self, s3_key):
        """
        Extract text from image stored in S3 using Textract
        Tries detect_document_text first, falls back to analyze_document if needed
        
        Args:
            s3_key: S3 object key
            
        Returns:
            str: Extracted text, or None if error
        """
        if not self.textract_client:
            self.logger.error("Textract client not initialized")
            return None
        
        try:
            # First try detect_document_text (simpler, faster, cheaper)
            try:
                response = self.textract_client.detect_document_text(
                    Document={
                        'S3Object': {
                            'Bucket': S3_BUCKET,
                            'Name': s3_key
                        }
                    }
                )
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'UnsupportedDocumentException':
                    self.logger.warning(f"[TEXTRACT] detect_document_text unsupported format for {s3_key}, trying analyze_document...")
                    # Try analyze_document as fallback (supports more formats)
                    try:
                        response = self.textract_client.analyze_document(
                            Document={
                                'S3Object': {
                                    'Bucket': S3_BUCKET,
                                    'Name': s3_key
                                }
                            },
                            FeatureTypes=['TABLES', 'FORMS']
                        )
                    except ClientError as e2:
                        self.logger.error(f"[TEXTRACT] analyze_document also failed: {e2}")
                        return None
                else:
                    raise
            
            extracted_text = []
            for block in response.get('Blocks', []):
                if block.get('BlockType') == 'LINE':
                    text = block.get('Text', '')
                    if text:
                        extracted_text.append(text)
            
            result = '\n'.join(extracted_text)
            if result.strip():
                self.logger.info(f"[TEXTRACT] Extracted {len(extracted_text)} lines from S3: {s3_key}")
                return result
            else:
                self.logger.warning(f"[TEXTRACT] No text found in image: {s3_key}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error extracting text from S3 with Textract: {e}")
            return None
    
    def process_attachment(self, file_url_or_path, source, file_id, filename, download=True):
        """
        Complete processing pipeline for an attachment:
        1. Download file (if URL)
        2. Upload to S3
        3. Extract text using Textract
        4. Clean up temporary files
        
        Args:
            file_url_or_path: URL to download or local file path
            source: 'slack' or 'jira'
            file_id: Unique identifier for the file
            filename: Original filename
            download: Whether to download from URL (True) or use local path (False)
            
        Returns:
            dict: {
                's3_url': str or None,
                'extracted_text': str or None,
                'success': bool
            }
        """
        temp_file = None
        
        try:
            # Step 1: Download or use local file
            if download:
                # Download from URL
                self.logger.info(f"[ATTACHMENT] Downloading {filename} from {file_url_or_path}")
                response = requests.get(file_url_or_path, timeout=30)
                response.raise_for_status()
                
                # Save to temporary file
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
                temp_file.write(response.content)
                temp_file.close()
                local_path = temp_file.name
            else:
                # Use provided local path
                local_path = file_url_or_path
            
            # Convert image to JPEG format if needed (before uploading to S3)
            # This ensures Textract compatibility
            converted_path = local_path
            if self._is_image_file(filename):
                try:
                    # Read file bytes fresh
                    with open(local_path, 'rb') as f:
                        img_bytes = f.read()
                    
                    # Validate file is not empty
                    if not img_bytes or len(img_bytes) == 0:
                        self.logger.warning(f"[ATTACHMENT] File is empty, skipping conversion")
                        converted_path = local_path
                    else:
                        # Validate file headers to check if it's actually an image
                        # Check PNG signature
                        is_png = img_bytes[:8] == b'\x89PNG\r\n\x1a\n'
                        # Check JPEG signature
                        is_jpeg = img_bytes[:2] == b'\xff\xd8'
                        
                        if not (is_png or is_jpeg):
                            self.logger.warning(f"[ATTACHMENT] File doesn't appear to be a valid PNG/JPEG (headers don't match), using original...")
                            converted_path = local_path
                        else:
                            # Create fresh BytesIO from bytes and reset position
                            img_buffer = io.BytesIO(img_bytes)
                            img_buffer.seek(0)  # Ensure position is at start
                            
                            try:
                                img = Image.open(img_buffer)
                                # Force PIL to verify the image
                                img.verify()  # Verify it's a valid image
                                
                                # Reopen after verify (verify() closes the image)
                                img_buffer.seek(0)
                                img = Image.open(img_buffer)
                                
                                # Verify image format
                                if not img.format:
                                    self.logger.warning(f"[ATTACHMENT] Could not identify image format after verification, using original...")
                                    img.close()
                                    converted_path = local_path
                                else:
                                    self.logger.debug(f"[ATTACHMENT] Image verified: format={img.format}, mode={img.mode}, size={img.size}")
                                    
                                    # Convert problematic formats to JPEG
                                    if img.format == 'PNG' and img.mode in ('RGBA', 'LA', 'P'):
                                        self.logger.info(f"[ATTACHMENT] Converting {img.mode} PNG to RGB JPEG before upload...")
                                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                                        if img.mode == 'RGBA':
                                            rgb_img.paste(img, mask=img.split()[3])
                                        else:
                                            rgb_img.paste(img)
                                        
                                        # Save converted image to temp file
                                        converted_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
                                        rgb_img.save(converted_temp, format='JPEG', quality=95)
                                        converted_temp.close()
                                        converted_path = converted_temp.name
                                        self.logger.info(f"[ATTACHMENT] Converted to JPEG: {converted_path}")
                                        
                                        # Close images to free memory
                                        rgb_img.close()
                                        img.close()
                                    elif img.format not in ('JPEG', 'PNG'):
                                        self.logger.info(f"[ATTACHMENT] Converting {img.format} to JPEG before upload...")
                                        if img.mode != 'RGB':
                                            img = img.convert('RGB')
                                        converted_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
                                        img.save(converted_temp, format='JPEG', quality=95)
                                        converted_temp.close()
                                        converted_path = converted_temp.name
                                        img.close()
                                    else:
                                        # Valid JPEG or PNG, no conversion needed
                                        img.close()
                                        converted_path = local_path
                            except Exception as img_open_error:
                                self.logger.warning(f"[ATTACHMENT] PIL could not open image: {img_open_error}, using original...")
                                converted_path = local_path
                except Exception as conv_error:
                    self.logger.warning(f"[ATTACHMENT] Could not convert image: {conv_error}, using original...")
                    import traceback
                    self.logger.debug(f"[ATTACHMENT] Conversion error: {traceback.format_exc()}")
                    converted_path = local_path
            
            # Step 2: Upload to S3 (use converted path if available)
            s3_key = self._generate_s3_key(source, file_id, filename)
            s3_url = self.upload_to_s3(converted_path, s3_key)
            
            # Clean up converted file if different from original
            if converted_path != local_path and os.path.exists(converted_path):
                try:
                    os.unlink(converted_path)
                except:
                    pass
            
            if not s3_url:
                self.logger.warning(f"[ATTACHMENT] Failed to upload {filename} to S3")
                return {
                    's3_url': None,
                    'extracted_text': None,
                    'success': False
                }
            
            # Step 3: Extract text using Textract (if image)
            # Note: Even if extraction fails, we still return success=True because S3 upload succeeded
            # The incident should still be processed with the message text, just without image context
            extracted_text = None
            if self._is_image_file(filename):
                # Try extracting from S3 first (more efficient)
                extracted_text = self.extract_text_from_s3(s3_key)
                
                # Fallback to local file if S3 extraction fails
                if not extracted_text:
                    self.logger.info(f"[ATTACHMENT] S3 extraction failed, trying local file extraction...")
                    extracted_text = self.extract_text_from_local_file(local_path)
                
                if extracted_text:
                    self.logger.info(f"[ATTACHMENT] ✅ Successfully extracted {len(extracted_text)} chars from image")
                else:
                    self.logger.warning(f"[ATTACHMENT] ⚠️ Could not extract text from image, but S3 upload succeeded. Incident will be processed without image context.")
            else:
                self.logger.info(f"[ATTACHMENT] {filename} is not an image format, skipping text extraction")
            
            # Return success=True even if text extraction failed (S3 upload succeeded)
            return {
                's3_url': s3_url,
                'extracted_text': extracted_text,  # None if extraction failed, but that's OK
                's3_key': s3_key,
                'success': True  # Success because S3 upload worked
            }
            
        except Exception as e:
            self.logger.error(f"Error processing attachment {filename}: {e}")
            return {
                's3_url': None,
                'extracted_text': None,
                'success': False,
                'error': str(e)
            }
        
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except Exception as e:
                    self.logger.warning(f"Could not delete temp file {temp_file.name}: {e}")


# Singleton instance
_processor_instance = None

def get_attachment_processor():
    """Get or create singleton AWS attachment processor instance"""
    global _processor_instance
    if _processor_instance is None:
        _processor_instance = AWSAttachmentProcessor()
    return _processor_instance

