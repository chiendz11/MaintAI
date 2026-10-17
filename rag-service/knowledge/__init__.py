# Import class KnowledgeProcessor từ file processor.py cùng thư mục
from .processor import KnowledgeProcessor

# Khai báo rõ các thành phần được phép "xuất" ra ngoài khi dùng câu lệnh: from knowledge import *
__all__ = ["KnowledgeProcessor"]