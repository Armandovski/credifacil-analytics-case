# Credifácil - Case de Análise de Crédito

## Introdução

Uma fintech fictícia — "Credifácil" — oferece crédito pessoal não consignado para clientes de diferentes perfis. A empresa quer evoluir sua capacidade de análise para:

- Monitorar a performance da carteira de crédito
- Antecipar riscos de inadimplência
- Avaliar a efetividade das políticas de concessão

Você foi contratado como Analyst Engineer para estruturar dados brutos, criar modelos analíticos e métricas de performance de crédito que apoiem a tomada de decisão dos times de Risco, Cobrança e Produto.

## Materiais Fornecidos

- **loans.csv** – histórico de contratos de crédito:
  - `loan_id`, `customer_id`, `data_concessao`, `valor_contratado`, `prazo_meses`, `taxa_juros_anual`, `canal`, `status` (ativo, liquidado, inadimplente, cancelado)

- **customers.csv** – dados cadastrais dos clientes:
  - `customer_id`, `nome`, `data_nascimento`, `renda_mensal`, `score_interno`, `estado`, `data_cadastro`

- **payments.csv** – histórico de pagamentos de parcelas:
  - `payment_id`, `loan_id`, `data_vencimento`, `data_pagamento`, `valor_parcela`, `valor_pago`, `atraso_dias`

## Desafio

### Modelagem e Transformação de Dados

Construa um modelo analítico consolidado que permita responder a perguntas de performance da carteira, unindo:

- Clientes
- Empréstimos
- Pagamentos

Trate e documente problemas comuns:

- Atrasos ou pagamentos parciais
- Contratos cancelados ou inadimplentes
- Clientes com múltiplos contratos
- Dados inconsistentes (ex.: datas invertidas, valores negativos)

🎯 **Avalia-se**: clareza da modelagem, qualidade das transformações e boas práticas de engenharia.

Crie e documente pelo menos 5 métricas-chave de risco e performance, como por exemplo:

- **Taxa de Inadimplência**: % de contratos com atraso > 90 dias ou status inadimplente
- **PAR 30 / PAR 90**: Portfolio at Risk — saldo em atraso com mais de 30/90 dias
- **Yield Efetivo**: Receita financeira realizada / saldo médio da carteira
- **Vintages de Inadimplência**: Taxa de default por mês de originação
- **% de clientes com múltiplos contratos**: Indica concentração de risco em clientes recorrentes

📊 **Avalia-se**: a capacidade de traduzir risco e performance em indicadores acionáveis.