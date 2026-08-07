# Dashboard

Mục tiêu: trực quan hoá kết quả từ `transform/models/marts/` (daily returns, moving averages, volatility, volume anomaly) cho người dùng cuối, không cần biết SQL/BigQuery.

## Vì sao Metabase trước, Svelte sau

Metabase kết nối trực tiếp BigQuery và dựng được dashboard trong vài phút, không cần viết code — phù hợp để có demo nhanh, xác nhận marts trả về đúng dữ liệu trước khi đầu tư thời gian vào UI riêng.

Trade-off: Metabase generic, ai cũng làm được, không phải điểm khác biệt khi phỏng vấn. Một Svelte app riêng (tái sử dụng skill sẵn có) tốn thời gian hơn nhưng thể hiện được full-stack — đây là việc mở rộng nếu còn thời gian sau khi pipeline chạy ổn định, không phải phần bắt buộc.

## Nội dung dashboard dự kiến

- Giá đóng cửa + MA20/MA50 theo symbol (`fct_moving_averages`)
- % thay đổi giá hàng ngày (`fct_daily_returns`)
- Volatility 20 ngày annualized theo symbol (`fct_volatility`)
- Danh sách ngày có volume bất thường (`fct_volume_anomaly`)
