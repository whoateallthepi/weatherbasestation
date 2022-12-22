def kph_to_mph (kph):
    mph = float(kph) * 1.609344
    return round(mph,2)

def celsius_to_f (celsius):
    f = (float(celsius) * 1.8) + 32
    return round(f,2)

def limit_percent (percent):
    if percent > 100:
        return round(100.00,2)
    else:
        return round(percent,2) 

def hpa_to_inches (hpa):
    inches = 0.02953 * float(hpa)
    return round(inches,4)

def mm_to_inches (mm):
    inches = float(mm) * 0.0393701
    return round(inches,4)