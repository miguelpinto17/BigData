# SETUP

## First time setup
Run `./setup_dev_env.sh`.
If permission error, run `chmod 700 setup_dev_env.sh` and try again.
If running on windows, make sure to run using WSL and check directly UV docs.

## Ongoing setup.
- Make sure environment is up to date running `uv sync`
- When to add a dependency, do: `uv add <dependency>`. Make sure not to use another dependency manager.
- Make sure VScode/IDE and environment python variables are pointing to ./venv.
- When running directly on terminal use `uv run <file>` or activate the environment before with `source .venv/bin/activate`

to-do list:
- all done!

Link do Projeto: https://www.overleaf.com/project/67ab28177d79699953f2f9c3


novo_fastfood.csv -> possui o nome de todos os restaurantes de fastfood, por estado, e as suas localizações
novo_obesity.csv -> possui o nome do state, index de obesidade, area e perimetro
fastfood_obesity -> possui state, Obesity_index, Area, Perimeter, fast_food_count, fast_food_density, fast_food_density_normalized. usou-se para fazer a ligação para o csv final, que se torna mais facil de analisar.

final.csv -> possui o state, obesity index e a densidade de fast food por estado 
