# sql-workbench
py script that exports csv files for an api

# proof-of-concept
```py
def main() -> None:
    """
    entry point/proof of concept:
    """
    # logging settings
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
    # instantiate
    converter = Export()
    # load csv
    converter.load_csv()
    # get a clean dataframe & export it
    converter.export(converter.clean())
    # debugging
    logging.info("[*] CSV successfully exported to %s [*]", converter.db_file)

if __name__ == "__main__":
    main()
```

![image](https://github.com/user-attachments/assets/7e4d2d13-aa95-4bad-bfb7-a0efda2e3443)
