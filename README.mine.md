# Airflow & Astro CLI

## Astro CLI
- instalado no windows mesmo via linha de comando com winget:
    - `winget install -e --id Astronomer.Astro`
    - buildar o airflow de forma mais simples

### Iniciando o astrocli:
- Após instalado, no repositório basta executar `astro dev init`
- Será criado as seguintes pastas no diretório:
```powershell
    Diretório: ...\projeto-airflow-dbt
Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----        21/01/2026     22:49                .astro
d-----        21/01/2026     22:49                dags
d-----        21/01/2026     22:49                include
d-----        21/01/2026     22:49                plugins
d-----        21/01/2026     22:49                tests
-a----        21/01/2026     22:49             73 .dockerignore
-a----        21/01/2026     22:49              0 .env
-a----        21/01/2026     22:49            132 .gitignore
-a----        21/01/2026     22:49            866 airflow_settings.yaml
-a----        21/01/2026     22:49             45 Dockerfile
-a----        21/01/2026     22:49              0 packages.txt
-a----        21/01/2026     22:49            155 requirements.txt
```
### Comandos Astro:
1. Iniciar o diretorio: `astro dev init`
2. Startar o servico: `astro dev start`
    - Executa o airflow (necessario o docker ativo)
3. Parar de rodar: `astro dev stop`
4. Reiniciar o servico: `astro dev restart`
5. Rodar comandos bash no container: `astro dev bash` && `ls`
6. Encerrar o container e deletar todo os metadados e dados do database: `astro dev kill`
7. Rodar determinada dag: `astro run <dag-id>`

### Conexao com postgres:
- Adicionado no requirements.txt:
    - apache-airflow-providers-postgres
    - psycopg2-binary
- Conexao criada diretamente via UI do Airflow:
    - connectionid: postgres_raw
    - postgres / postgres / postgres / porta 5432


### Execucao DAG:
- Conecta carrega os dados, salva local no container, cria as tabelas, pega os daos local e append nas tabelas.
![ExecutionDone_step1](assets/images/dag_success_execution.png)

# Referência:
- [Video do DataWay](https://www.youtube.com/watch?v=cET2DwVhnc4)