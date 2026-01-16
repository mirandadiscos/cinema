# Plotando Gráficos com Dados do Letterboxd

Este projeto combina Engenharia de Dados e Desenvolvimento Backend para visualizar e analisar dados do Letterboxd. O script principal lê seus dados de filmes, os enriquece com informações da API do The Movie Database (TMDb) e salva o resultado em um novo arquivo CSV.

## Instalação e Execução

Siga os passos abaixo para configurar e executar o projeto.

### 1. Pré-requisitos
- Python 3.10+
- `pip` e `venv`

### 2. Instalação
1.  **Clone o repositório e crie um ambiente virtual:**
    ```bash
    # git clone <repository-url>
    # cd <repository-directory>
    python3 -m venv .venv
    source .venv/bin/activate
    ```

2.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

### 3. Configuração
1.  **Crie o arquivo de ambiente:**
    Crie um arquivo chamado `.env` na raiz do projeto.

2.  **Adicione sua chave da API do TMDb:**
    ```
    TMDB_API_KEY=sua_chave_aqui
    ```

### 4. Execução

**Primeiro, ative o ambiente virtual:**
```bash
source .venv/bin/activate
```

Para iniciar o processo de enriquecimento de dados, execute o seguinte comando. Ele irá carregar os dados de `data_input/reviews.csv`, enriquecê-los e salvar o resultado em `data_input/enriched_data.csv`.

```bash
python data_processing/load_data.py
```

## Executando os Testes

Para garantir que a lógica de processamento está funcionando corretamente (sem depender de chamadas reais à API), execute os testes unitários com o seguinte comando (com o venv ativado):

```bash
python -m unittest data_processing/test_enrich_data.py
```

<details>
<summary><strong>Detalhes do Projeto e Próximos Passos</strong></summary>

## Plano do Projeto

Nosso objetivo é criar uma aplicação completa para análise de dados do Letterboxd.

### Fase 1: Engenharia de Dados (Preparação)
- [x] Explorar e entender a estrutura dos seus dados Letterboxd (CSV).
- [x] Carregar e limpar os dados principais usando Python e Pandas.
- [x] Enriquecer os dados dos filmes buscando informações adicionais (gênero, país, diretor, etc.) de APIs externas, como o TMDb.
- [ ] Desenvolver a lógica de agregação para calcular estatísticas e rankings desejados (ex: top 5 por ano, gênero, diretor, etc.).

### Fase 2: Backend (Servir os Dados)
- [ ] Configurar uma API usando FastAPI para expor os dados processados e agregados.
- [ ] Criar diferentes endpoints na API para cada tipo de dado ou estatística que o frontend precisará.

### Fase 3: Frontend (Visualização)
- [ ] Desenvolver uma interface web simples para consumir os dados da API e apresentá-los visualmente.

## Melhorias Futuras

### TODO: Tradução da Sinopse
A sinopse dos filmes (`Synopsis`) é atualmente obtida em inglês. Uma melhoria importante seria adicionar uma etapa de tradução para o português.

Existem duas abordagens principais:

1.  **APIs de Tradução (Recomendado para Produção):**
    - **Serviços:** Google Cloud Translation, DeepL API.
    - **Prós:** Traduções de alta qualidade, confiáveis e robustas.
    - **Contras:** Requerem configuração de chaves de API e podem ter custos associados.

2.  **Bibliotecas Gratuitas (Para Desenvolvimento/Prototipagem):**
    - **Ferramenta:** `googletrans`.
    - **Prós:** Fácil de implementar para testes rápidos, sem necessidade de chave de API.
    - **Contras:** É uma biblioteca não oficial, o que a torna instável e inadequada para um ambiente de produção.

</details>
