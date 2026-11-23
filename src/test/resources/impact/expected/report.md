## Change impact

`origin/master (abc1234)` -> `working tree`

**1 direct, 1 impacted, 1 likely failure, breaking: yes**

### Direct changes

| Class | Kind | Node | Columns | Details |
|---|---|---|---|---|
| BREAKING | load | `raw.orders` | amount | column amount removed |

### Impacted downstream

| Class | Kind | Node | Columns | Path | |
|---|---|---|---|---|---|
| BREAKING | task | `stg.orders` | * | raw.orders[amount] -> stg.orders | likely failure |

### Unresolved

- `stg.orders` -> `kpi.py`: python task

### Other changes, not traced

- dags/default.sl.yml (changed)
