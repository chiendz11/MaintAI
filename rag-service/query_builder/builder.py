from api.schemas import MaintenanceBriefRequest

class QueryBuilder:
    def build(self, request: MaintenanceBriefRequest) -> str:
        """
        Tạo câu truy vấn tìm kiếm tài liệu bảo trì
        từ dữ liệu cảnh báo và dự đoán.
        """

        alert = request.alert
        prediction = request.prediction
        sensor = request.sensor_context

        parts = [
            f"Industrial machine maintenance",
            f"Machine product type: {sensor.product_type}",
            f"Predicted failure: {prediction.prediction}",
            f"Alert severity: {alert.severity}",
        ]

        if request.intent.value == "TECHNICIAN_CHECKLIST":
            parts.append(
                "Inspection steps, troubleshooting and maintenance procedures"
            )

        elif request.intent.value == "ALERT_EXPLANATION":
            parts.append(
                "Possible causes, symptoms and explanation of machine failure"
            )

        elif request.intent.value == "MAINTENANCE_BRIEF":
            parts.append(
                "Failure causes, recommended maintenance and safety precautions"
            )

        return ". ".join(parts)
