mkdir -p logs

LOG="logs/faprotax_inputs_$(date +%Y%m%d_%H%M%S).log"

nohup python3 -u \
  ../echorepo-lite-dev/tools/generate_biodiversity_piecharts.py \
  --marker 16S \
  --build-faprotax-inputs \
  > "$LOG" 2>&1 &

echo "PID=$!"
echo "LOG=$LOG"
