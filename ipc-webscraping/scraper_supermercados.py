import os

scripts = ["scraper_plazavea.py",
    "scraper_metro.py",
    "scraper_vivanda.py",
    "scraper_wong.py"
]

for script in scripts:
    print(f"\n Ejecutando {script}...")
    os.system(f"python {script}")
