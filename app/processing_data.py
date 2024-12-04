import glob
import time
from cassandra.cluster import Cluster
import os

# doc tat ca cac data cu
list_old = glob.glob("/home/snowfox/Documents/kafka/output/*.csv")

# Thêm try-except cho phần kết nối Cassandra
try:
    cluster = Cluster()
    session = cluster.connect('k1')
    print("Kết nối Cassandra thành công!")
except Exception as e:
    print(f"Lỗi khi kết nối Cassandra: {str(e)}")
    raise  # Dừng chương trình nếu không kết nối được

columns = ['ID' ,'QUARTER' ,'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 
            'OP_UNIQUE_CARRIER',
            'ORIGIN',
            'DEST','DISTANCE',
            'CRS_DEP_TIME', 
            'LABEL','prediction']

dataframes_list_old = []
dataframes_list_total_old = [] 
import pandas as pd
for i in range(len(list_old)):
    dataframes_list_old = pd.read_csv(list_old[i], names = columns)
    dataframes_list_total_old.append(dataframes_list_old)
    
df = pd.concat(dataframes_list_total_old).reset_index(drop=True)

# Thêm try-except cho phần insert dữ liệu ban đầu
try:
    for i in range(len(df)):
        query = "Insert into stream_data (id,quarter,month,day_of_month, day_of_week, \
                op_unique_carrier,origin, dest,distance, crs_dep_time, \
                label) values ("+"'"+str(df.iloc[:,0][i])+"'"+","+str(df.iloc[:,1][i])+"," \
                +str(df.iloc[:,2][i])+","+str(df.iloc[:,3][i])+","+str(df.iloc[:,4][i])+","+"'"+ str(df.iloc[:,5][i])+"'"+\
                    ","+"'"+str(df.iloc[:,6][i])+"'"+","+"'"+str(df.iloc[:,7][i])+"'"+","+ \
                        str(df.iloc[:,8][i])+","+str(df.iloc[:,9][i])+","+str(df.iloc[:,10][i])+ ")"
        try:
            session.execute(query)
        except Exception as e:
            print(f"Lỗi khi insert dòng {i}:")
            print(f"Query: {query}")
            print(f"Lỗi: {str(e)}")
except Exception as e:
    print(f"Lỗi không xác định khi insert dữ liệu ban đầu: {str(e)}")

def get_csv_files():
    """Lấy danh sách các file CSV trong thư mục output"""
    files = glob.glob("/home/snowfox/Documents/kafka/output/*.csv")
    print(f"toàn bộ {len(files)} file CSV:")
    for f in files:
        pass
    return files

# Khởi tạo danh sách file ban đầu
list_old = get_csv_files()

if not list_old:
    print("Không tìm thấy file CSV trong thư mục output. Đang chờ dữ liệu mới...")
    list_old = []

while True:
    print("\n=== Bắt đầu vòng lặp mới ===")
    print("Đang chạy vòng lặp kiểm tra dữ liệu mới...")
    time.sleep(15)
    
    list_new = get_csv_files()
    new_files = []
    
    # So sánh từng file trong list_new với list_old
    for file_path in list_new:
        if file_path not in list_old:
            new_files.append(file_path)
    
    print("\nDanh sách file mới phát hiện:")
    if new_files:
        for f in new_files:
            file_time = os.path.getmtime(f)
            print(f"- {f}")
            print(f"  Thời gian tạo: {time.ctime(file_time)}")
    else:
        print("(Không có file mới)")
    
    print(f"\nSố file mới: {len(new_files)}")
    
    if new_files:
        dataframes_list_total = []
        for file_path in new_files:
            try:
                df = pd.read_csv(file_path, names=columns)
                dataframes_list_total.append(df)
            except Exception as e:
                print(f"Lỗi khi đọc file {file_path}: {str(e)}")
                continue
        
        if dataframes_list_total:
            df_stream = pd.concat(dataframes_list_total).reset_index(drop=True)
            print(f"Tổng số dòng dữ liệu mới: {len(df_stream)}")
            
            try:
                for i in range(len(df_stream)):
                    query = "Insert into stream_data (id,quarter,month,day_of_month, day_of_week, \
                        op_unique_carrier,origin, dest,distance, crs_dep_time, \
                        label) values ("+"'"+str(df.iloc[:,0][i])+"'"+","+str(df.iloc[:,1][i])+"," \
                        +str(df.iloc[:,2][i])+","+str(df.iloc[:,3][i])+","+str(df.iloc[:,4][i])+","+"'"+ str(df.iloc[:,5][i])+"'"+\
                            ","+"'"+str(df.iloc[:,6][i])+"'"+","+"'"+str(df.iloc[:,7][i])+"'"+","+ \
                                str(df.iloc[:,8][i])+","+str(df.iloc[:,9][i])+","+str(df.iloc[:,10][i])+ ")"
                    try:
                        session.execute(query)
                    except Exception as e:
                        print(f"Lỗi khi insert dòng {i} của dữ liệu mới:")
                        print(f"Query: {query}")
                        print(f"Lỗi: {str(e)}")
                print("Đã insert thành công vào Cassandra")
            except Exception as e:
                print(f"Lỗi không xác định khi insert dữ liệu mới: {str(e)}")

            try:
                df_stream.to_csv("/home/snowfox/Documents/kafka/data_final/display.csv", index = False)
                print("Đã lưu file display.csv thành công")
            except Exception as e:
                print(f"Lỗi khi lưu file display.csv: {str(e)}")
            
            list_old = list_new.copy()  # Cập nhật list_old bằng cách copy list_new
            print("Đã cập nhật danh sách file cũ")
    else:
        print("Không có file mới")
