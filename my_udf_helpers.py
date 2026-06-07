def check_congestion_logic(speed, distance):
    if speed is None or distance is None:
        return 0
    if speed < 10.0 and distance >= 1.0:
        return 1
    return 0
