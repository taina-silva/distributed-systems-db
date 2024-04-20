# distributed-systems-db

Para compilar o projeto, execute:

```bash
chmod +x compile.sh && ./compile.sh;
```

Dessa vez, tivemos alguns problemas na implementação do trabalho, em especial:

- `O cache local não foi totalmente implementado.`
    - `Nossa ideia seria eventualmente usar os métodos do trabalho anterior usando os hashs de cada servidor (como não era o principal dessa segunda etapa, não demos prioridade nisso).`
- `Não conseguimos garantir consistência total nos casos de operação de remoção de algum aluno e/ou professor (pois, nesse caso, remoção deveriam encadear outras operações de remoção para garantia a consistência, que não conseguimos garantir 100%).`
    - `Acreditamos que um grande detrator nesse sentido foi a péssima escolha da estrutura dos dados na primeira etapa, que trouxe uma complexidade extra que percebemos posteriormente desnecessária.`
- `Um exemplo dessa eventual quebra de consistência seria a permanência de um eventual ID de professor que foi excluído anteriormente, mas que na hora de imprimir não mostra.`
- `Dessa vez não deu para testar todo o suite de testes, então *infelizmente* não conseguimos garantir 100% de consistência! Uma pena!`
- `Nós sabemos os passos que deveriam ser tomados, porém não deu tempo para implementarmos tudo (vide o histórico de commits, Tainá principalmente ficou até 6h da madrugada tentando resolver esse maravilhoso trabalho --- obrigado, Tainá!).`
    - `Nesse sentido, uma questão que atrasou muito foi ter que levantar essa quantidade de servidores toda vez que havia alguma falha.`
