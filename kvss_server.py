import socket, time

HOST = "127.0.0.1"
PORT = 5050
PROTOCOL = "KV/1.0"
BUFFER_SIZE = 1024
MAX_LINE= 8 * 1024  # gioi han 1 dong lenh ~8KB

#kho lưu trữ KV &cac bien thong ke
store = {}                        #dictionary luu cac cap key-value
start_time= time.time()          #thoi diem server khoi dong (tinh uptime)
served_count =0                  #dem so request đa pvu

#tao socket tcp lang nghe tren cong 5050
with socket.socket (socket.AF_INET, socket.SOCK_STREAM) as server_sock:
    server_sock.bind ((HOST, PORT))
    server_sock.listen()
    print (f"KVSS Server listening on {HOST}:{PORT}...")
    #cho &lien tuc chap nhan ket noi
    
    while True:
        conn,addr = server_sock.accept()  #acp new connection
        print(f"[+] Accepted connection from {addr}")
        
        with conn:
            while True:
                try:
                    #ddọc dlieu den khi gap n'
                    data = b""
                    while not data.endswith(b"\n"):
                        chunk =conn.recv(1024)
                        if not chunk:      #knoi bi dong thi
                            break
                        data += chunk
                    if not data:
                        break  #thoat loop neu client dong ket noi
                    line =data.decode('utf-8').strip()  # Chuoi lennh (bo '\n')
                except Exception as e:
                    print("[-] Error receiving data:", e)
                    break

                #log request voi timestamp
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(f"[{timestamp}] Request: {line}")

                response= ""  #chbi phan hoi
                #check prefix KV/1.0
                if not line.startswith("KV/1.0"):
                    response= "426 UPGRADE_REQUIRED"
                else:
                    #tach thanh [han lenh va tham so
                    parts = line.split(" ", 2)  #max 3
                    #parts[0]= "KV/1.0",parts[1]= COMMAND, parts[2]=phan con lai(args)
                    if len(parts) < 2:
                        response = "400 BAD_REQUEST"
                    else:
                        cmd = parts[1]
                        if cmd == "PUT":
                            #chuoi có dang: KV/1.0 PUT key value
                            if len(parts) < 3 or " " not in parts[2]:
                                response = "400 BAD_REQUEST"
                            else:
                                #tach key va value tu phan args
                                key , value = parts[2].split(" ", 1)
                                if key == "" or value == "":
                                    response = "400 BAD_REQUEST"
                                else:
                                    if key in store:
                                        store[key] = value
                                        response = "200 OK"
                                    else:
                                        store[key] = value
                                        response = "201 CREATED"
                        elif cmd == "GET":
                            if len(parts) < 3:
                                response = "400 BAD_REQUEST"
                            else:
                                key = parts[2]
                                if key in store:
                                    response = f"200 OK {store[key]}"
                                else:
                                    response = "404 NOT_FOUND"
                        elif cmd == "DEL":
                            if len(parts) < 3:
                                response = "400 BAD_REQUEST"
                            else:
                                key = parts[2]
                                if key in store:
                                    store.pop(key)  #xoa key
                                    response = "204 NO_CONTENT"
                                else:
                                    response = "404 NOT_FOUND"
                        elif cmd == "STATS":
                            #tinh  ttin thke
                            uptime = int(time.time() - start_time)
                            response = (f"200 OK keys={len(store)} "
                                        f"uptime={uptime}s served={served_count}")
                        elif cmd == "QUIT":
                            response = "200 OK bye"
                        else:
                            #k hop le
                            response = "400 BAD_REQUEST"
                #+1 req da phuc vu
                served_count += 1

                #log phan hoi
                print(f"[{timestamp}] Response: {response}")
                #gui phan hoi
                try:
                    conn.sendall((response + "\n").encode('utf-8'))
                except Exception as e:
                    print("[-] Error sending response:", e)
                    break

                #QUIT-> thoat loop de dong ket noi
                if line.startswith("KV/1.0") and parts[1] == "QUIT":
                    break
        #ket thuc xli 1 client (ra khoi with conn)
        print(f"[-] Connection closed from {addr}")
