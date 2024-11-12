import asyncio
from asyncio import WindowsSelectorEventLoopPolicy
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

notebooks = [
    "dim_cliente.ipynb", 
    "dim_fecha.ipynb", 
    "dim_hora.ipynb", 
    "dim_mensajero.ipynb",
    "dim_sede.ipynb",
    "dim_tipo_novedad.ipynb",
    "hecho_entrega_mensajeria_acumulada.ipynb",
    "hecho_entrega_mensajeria_diario.ipynb",
    "hecho_entrega_mensajeria_hora.ipynb",
    "hecho_novedad.ipynb",
    "constraints.ipynb"
]

def run_notebook(notebook_path):
    try:
        with open("notebook/" + notebook_path) as f:
            nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        ep.preprocess(nb, {"metadata": {"path": "./notebook/"}})
        print(f"Notebook {notebook_path} executed successfully")
        return True
    except Exception as e:
        print(f"Error executing {notebook_path}: {e}")
        return False

def main():
    print("Running notebooks...")
    all_success = True

    for notebook in notebooks:
        success = run_notebook(notebook)
        if not success:
            all_success = False

    if all_success:
        print("All notebooks executed successfully")
    else:
        print("Some notebooks did not execute successfully")


if __name__ == "__main__":
    main()