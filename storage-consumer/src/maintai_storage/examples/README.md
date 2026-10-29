Fixtures tổng hợp để kiểm tra wire schema; bucket/version/checksums chỉ là ví dụ, không dùng deploy.
Segment có timestamps trùng, duplicate source event khác locator và rejected thiếu máy.
Các ca lag/gap/corruption/restart/CAS được kiểm trong tests/test_catalog.py và integration.
