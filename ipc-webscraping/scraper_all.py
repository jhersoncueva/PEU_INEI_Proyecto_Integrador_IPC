import os
import time

# python scraper_all.py
scripts = [
    "scraper_plazavea.py",
    "scraper_metro.py",
    "scraper_vivanda.py",
    "scraper_wong.py",
    "scraper_inkafarma.py",
    "scraper_mifarma.py",
    "scraper_osinergmin.py",
    "scraper_tayloy.py",
    "scraper_urbania.py",
]

start = time.time()

for script in scripts:
    print(f"\n Ejecutando {script}...")
    os.system(f"python {script}")


end = time.time()

demora = (end - start) / 60

print(f"Tiempo total: {demora:.2f} minutos")
