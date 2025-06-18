RUST_LOG=debug cargo run -- --backend g-streamer --socket /tmp/mysnd.socket

cargo run -- --backend alsa --socket /tmp/snd.sock 


qemu-system-x86_64 \
  -mem-prealloc \
  -object memory-backend-memfd,share=on,id=mem0,size=4G \
  -machine q35,memory-backend=mem0,accel=kvm \
  -chardev socket,id=vsnd,path=/tmp/mysnd.socket \
  -device vhost-user-snd-pci,chardev=vsnd,id=snd \
  -drive file=os.qcow2,format=qcow2,if=virtio \
  -boot order=c

