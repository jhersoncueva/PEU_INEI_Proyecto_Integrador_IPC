import os
import time

# python scraper_all.py
scripts = [
    "wscraper/scraper_plazavea.py",
    "wscraper/scraper_metro.py",
    "wscraper/scraper_vivanda.py",
    "wscraper/scraper_wong.py",
    "wscraper/scraper_inkafarma.py",
    "wscraper/scraper_mifarma.py",
    "wscraper/scraper_osinergmin.py",
    "wscraper/scraper_tayloy.py",
    "wscraper/scraper_urbania.py",
]

start = time.time()

for script in scripts:
    print(f"\n Ejecutando {script}...")
    os.system(f"python {script}")


end = time.time()

demora = (end - start) / 60

print(f"Tiempo total: {demora:.2f} minutos")
