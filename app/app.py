from tkinter import *  # Nhập tất cả các thành phần từ thư viện Tkinter
import datetime  # Nhập thư viện datetime để xử lý thời gian

import pandas as pd  # Nhập thư viện pandas để thao tác với dữ liệu
import numpy as np  # Nhập thư viện numpy cho các phép toán số học

# Đọc dữ liệu từ file CSV và lưu vào DataFrame 'data'
data = pd.read_csv('/home/snowfox/Documents/kafka/data_final/display.csv')

def doiso(df): 
    # Sử dụng loc để tránh cảnh báo SettingWithCopyWarning
    mask_0 = df['prediction'] == 0.0
    mask_1 = df['prediction'] == 1.0 
    mask_2 = df['prediction'] == 2.0
    
    df.loc[mask_0, 'prediction'] = "Not Delay"
    df.loc[mask_1, 'prediction'] = "Delay 30m"
    df.loc[mask_2, 'prediction'] = "Delay more than 30m"

# Gọi hàm 'doiso' để chuyển đổi dữ liệu trong 'data'
doiso(data)

# Thêm hàm chuyển đổi thời gian
def format_time(time_float):
    try:
        # Chuyển đổi sang số nguyên để loại bỏ phần thập phân
        time_int = int(float(time_float))
        # Lấy giờ và phút
        hours = time_int // 100
        minutes = time_int % 100
        # Định dạng thành chuỗi HH:MM
        return f"{hours:02d}:{minutes:02d}"
    except:
        return time_float

# Tạo cửa sổ chính của ứng dụng Tkinter
window = Tk()
window.title("App")  # Đặt tiêu đề cho cửa sổ là "App"
window.configure()  # Cấu hình cửa sổ (có thể thêm các tùy chọn khác nếu cần)

# Tạo một nhãn (Label) để hiển thị "Departures" với font Arial kích thước 20
lbl = Label(window, text="Departures", font=("Arial", 20))
lbl.grid(column=0, row=0)  # Đặt vị trí của nhãn trong lưới (grid) tại cột 0, hàng 0

# Lấy thời gian hiện tại và định dạng nó
time = datetime.datetime.now().strftime("Time: %H:%M:%S")
# Tạo một nhãn để hiển thị thời gian với font Arial kích thước 20
lbl2 = Label(window, text=str(time), font=("Arial", 20))
lbl2.grid(column=4, row=0)  # Đặt vị trí của nhãn thời gian trong lưới tại cột 4, hàng 0

# Định nghĩa hàm 'clock' để cập nhật thời gian và dữ liệu hiển thị
def clock():
    data = pd.read_csv('/home/snowfox/Documents/kafka/data_final/display.csv')  # Đọc lại dữ liệu từ file CSV
    doiso(data)  # Gọi hàm 'doiso' để chuyển đổi dữ liệu prediction

    # Kiểm tra số lượng dòng dữ liệu
    num_rows = min(9, len(data))  # Lấy tối đa 9 dòng hoặc số dòng hiện có
    
    # Lấy thời gian hiện tại và định dạng
    time = datetime.datetime.now().strftime("Time: %H:%M:%S")
    # Cập nhật lại nội dung của nhãn "Chuyen Bay/Departures"
    lbl = Label(window, text="Chuyen Bay/Departures", font=("Arial", 20))
    lbl.grid(column=0, row=0)  # Đặt vị trí mới cho nhãn trong lưới

    # Cập nhật lại thời gian hiển thị
    time = datetime.datetime.now().strftime("Time: %H:%M:%S")  # Lấy lại thời gian hiện tại
    lbl2 = Label(window, text=str(time), font=("Arial", 20))  # Tạo nhãn mới với thời gian cập nhật
    lbl2.grid(column=4, row=0)  # Đặt vị trí của nhãn thời gian trong lưới

    # Định nghĩa màu nền và màu chữ cho tiêu đề
    header_bg_color = '#8A1212'  # Màu nền đỏ đậm cho tiêu đề
    header_fg_color = '#FFFFFF'  # Màu chữ trắng cho tiêu đề

    # Định nghĩa màu nền và màu chữ cho các dòng dữ liệu
    color = ['#000000', '#3a4a6d']  # Màu nền đen và xám xanh
    color_f = ['#FFFFFF', '#FFFFFF']  # Màu chữ trắng cho cả hai màu nền

    # Tạo các nhãn tiêu đề cho cột dữ liệu
    for j in range(5):
        if j == 0:
            l = Label(window, text=str(data.columns[0]), relief=RIDGE, bg=header_bg_color, fg=header_fg_color, pady=15, padx=50, font=("Arial", 10, "bold"))
        elif j == 1:
            l = Label(window, text=str(data.columns[6]), relief=RIDGE, bg=header_bg_color, fg=header_fg_color, pady=15, padx=50, font=("Arial", 10, "bold"))
        elif j == 2:
            l = Label(window, text=str(data.columns[7]), relief=RIDGE, bg=header_bg_color, fg=header_fg_color, pady=15, padx=50, font=("Arial", 10, "bold"))
        elif j == 3:
            l = Label(window, text=str(data.columns[9]), relief=RIDGE, bg=header_bg_color, fg=header_fg_color, pady=15, padx=50, font=("Arial", 10, "bold"))
        elif j == 4:
            l = Label(window, text=str(data.columns[11]), relief=RIDGE, bg=header_bg_color, fg=header_fg_color, pady=15, padx=50, font=("Arial", 10, "bold"))
        l.grid(row=1, column=j, sticky=NSEW)  # Đặt vị trí của mỗi nhãn tiêu đề trong lưới

    # Tạo các nhãn hiển thị dữ liệu cho từng hàng và cột
    for i in range(num_rows):
        for j in range(5):
            if j == 0:
                l = Label(text=str(data.iloc[i, 0]), relief=RIDGE, bg=color[i % 2], fg=color_f[i % 2], pady=15, padx=50, font=10)
            elif j == 1:
                l = Label(text=str(data.iloc[i, 6]), relief=RIDGE, bg=color[i % 2], fg=color_f[i % 2], pady=15, padx=50, font=10)
            elif j == 2:
                l = Label(text=str(data.iloc[i, 7]), relief=RIDGE, bg=color[i % 2], fg=color_f[i % 2], pady=15, padx=50, font=10)
            elif j == 3:
                l = Label(text=format_time(data.iloc[i, 9]), relief=RIDGE, bg=color[i % 2], fg=color_f[i % 2], pady=15, padx=50, font=10)
            elif j == 4:
                l = Label(text=str(data.iloc[i, 11]), relief=RIDGE, bg=color[i % 2], fg=color_f[i % 2], pady=15, padx=50, font=10)
            l.grid(row=i+2, column=j, sticky=NSEW)  # Đặt vị trí của mỗi nhãn dữ liệu trong lưới
    window.after(15000, clock)  # Gọi lại hàm 'clock' sau 15000ms để cập nhật dữ liệu và thời gian

# Chạy lần đầu tiên hàm 'clock' để khởi động cập nhật dữ liệu và thời gian
clock()

mainloop()  # Bắt đầu vòng lặp chính của Tkinter để hiển thị cửa sổ