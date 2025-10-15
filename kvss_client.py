import socket

HOST= "127.0.0.1"
PORT= 5050
PROTOCOL = "KV/1.0"
BUFFER_SIZE = 1024
MAX_LINE= 8 * 1024  # gioi han 1 dong lenh ~8KB


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
    try:
        client_sock.connect ((HOST, PORT))
        print(f"Connected to KVSS server at {HOST}:{PORT}. Enter commands:")
    except Exception as e:
        print("Connection error:", e)
        exit(1)
    try:
        while True:
            user_input = input().strip()
            
            if not user_input:
                continue  #bo qua neu nhap rong
            #tu dong them KV/1.0 vao neu nguoi dung quen
            if not user_input.startswith("KV/1.0"):
                user_input = "KV/1.0 " + user_input
            #dam bao co \n khi gui
            message = user_input if user_input.endswith("\n") else user_input + "\n"
            
            try:
                client_sock.sendall(message.encode('utf-8'))
            except Exception as e:
                print ("Send failed:", e)
                break
            #nhan phan hoi den khi gap \n
            data = b""
            try:
                while not data.endswith(b"\n"):
                    chunk = client_sock.recv(1024)
                    if not chunk:
                        break
                    data += chunk
            except Exception as e:
                print("Receive error:", e)
                break
            if not data:
                print("Server closed the connection.")
                break
            response =data.decode('utf-8').strip()
            print(response)
            
            #Qui=>thoat
            if user_input.upper().startswith("KV/1.0 QUIT"):
                break
    except KeyboardInterrupt:
        print("\nClient interrupted by user.")
