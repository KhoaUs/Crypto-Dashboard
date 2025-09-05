# AI Finance Dashboard

Hệ thống **phân tích & dự đoán tài chính** dùng dữ liệu thị trường, tin tức và AI.  
Bao gồm các thành phần microservice (Docker Compose) và giao diện web realtime.

---

## Tính năng chính

- **Dashboard realtime**: hiển thị nến (candlestick) và chỉ báo (MA30, MA90).
- **Tin tức & sentiment**: crawler, sentiment service, và API phục vụ dashboard.
- **Backtest**:  
  - Chiến lược MA cross ± sentiment filter.  
  - Kết quả gồm số lệnh, winrate, **PnL (lãi/lỗ)** và **Equity curve (đường vốn)**, Sharpe ratio.  
- **Quản lý tài khoản**:
  - Đăng ký / đăng nhập / đăng xuất (JWT).
  - Đổi mật khẩu (rule: ≥8 ký tự, chữ hoa, chữ thường, số, ký tự đặc biệt).
  - Quản lý user (Admin): phân quyền, vô hiệu hoá, reset mật khẩu.  
  - *(Chưa hỗ trợ xóa user — có thể thao tác thủ công trong DB).*

---

## Cài đặt & Chạy

### Yêu cầu
- Docker & Docker Compose
- Git

### Clone repo
```bash
git clone https://github.com/KhoaUs/Crypto-Dashboard
cd Crypto-Dashboard
```

### Cấu hình `.env`
Copy file mẫu rồi chỉnh sửa giá trị phù hợp:
```bash
cp .env.example .env
```

### Build & Run
```bash
docker compose up --build
```

### Truy cập
- Dashboard: mở `index.html` bằng Live Server (VD: http://127.0.0.1:5500/index.html).
- Auth API: http://localhost:8080  
- Candle API: http://localhost:8000  
- News API: http://localhost:8100  
- Backtest API: http://localhost:8300  
- Realtime API (WS): ws://localhost:8400/ws

---

## Bảo mật

- Mật khẩu user hash bằng **bcrypt**.  
- Rule mật khẩu: tối thiểu 8 ký tự, phải có chữ hoa, thường, số, ký tự đặc biệt.  
- JWT với thời hạn ngắn (15 phút) + refresh token.  
- Admin có quyền phân quyền, disable, reset password.  

---


## License
MIT Lisence
