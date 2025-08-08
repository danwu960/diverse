# read files and file-names, generate csv for data input into Nodegoat.
import glob
import hashlib
import re
from datetime import datetime

import pandas as pd


def process_file(directory_path):
    """
    extract date and identify letters, for further processing with content.
    when working for different versions, change content column to content_v1, content_v2 and so on.
    :param directory_path:
    :return:
    """
    file_names = []
    content = []
    dates = []
    id = []
    page_id = []
    # read all the files i
    for f in glob.glob(directory_path + "*.txt"):
        base = f.split(".")[1].split("_")
        # dates with eventually number e.g. 19870724a
        id.append(base[0])
        page_id.append(base[2])
        ds = re.findall(r"\d+", base[0])
        dates.append(ds[len(ds) - 1])
        file_names.append(f.split('/')[1])
        the_content = ""
        with open(f, "r", encoding="utf-8") as file:
            the_content = file.read().strip()

        content.append(the_content)
    # change the version of the content
    df = pd.DataFrame(content, columns=['content_v3'])
    df['name'] = file_names
    # df['theme'] = ['peace' for i in range(0, len(file_names))]
    df['theme'] = ['peace' for i in range(0, len(file_names))]
    df['date'] = dates
    df['id'] = id  # dates
    df['page_id'] = page_id
    # df.to_csv('peace_old.csv', index=False)
    return df


def process_content(df):
    """
    look into the file names and combine two contents into one if they are
    one letter. Need to delete the empty lines or letters less than 5 lines
    in the beginning or end of the text.
    :param df: return from the function process_file
    :return: write the file into csv, the shape of the new file.
    """

    df_sorted = df.sort_values(['id', 'page_id'], ascending=True)
    aggregated = df_sorted.groupby('id').agg({
        'date': 'first', 'theme': 'first', 'name': 'first',
        'page_id': lambda x: '_'.join(x), 'content_v3': lambda x: '\n'.join(x)
    }).reset_index()
    new_df = aggregated.drop_duplicates(subset=['id'])
    # print(new_df.describe())
    return new_df


def extract_heading(df):
    """
    identify the pattern of the heading and extract the heading
    :param df:
    :return:
    """
    pattern_1 = r"(^\n|[^\n])(.*\S.*)\n\n"
    pattern_2 = r"(.*\n\n)(.*\S.*)$"
    df.loc[:, 'potential_heading'] = df.loc[:, 'content_v3'].apply(lambda x:
                                                                   re.search(
                                                                       pattern_1,
                                                                       str(x)).group() if re.search(
                                                                       pattern_1,
                                                                       str(x)) else str(
                                                                       x)[
                                                                                    0:70])
    df.loc[:, 'potential_heading'] = df.loc[:, 'potential_heading'].apply(
        lambda x:
        x.replace("\n", ""))
    df.loc[:, 'potential_note'] = df.loc[:, 'content_v3'].apply(lambda x: list(
        re.finditer(
            pattern_2, str(x)))[-1].group() if list(re.finditer(pattern_2,
                                                                str(x))) else str(
        x)[len(x) - 30:len(x) - 1])
    df.loc[:, 'potential_note'] = df.loc[:, 'potential_note'].apply(lambda x:
                                                                    x.replace(
                                                                        "\n",
                                                                        ""))
    # be careful with the file name
    df.to_csv('peace3.csv', index=False)
    return df


def merge_df(d1, d2, d3, key="id", how="outer"):
    """
    merge the three versions of df, and how to check which headings is better???
    :return:
    """
    middle = pd.merge(d1, d2, on=key, how="outer")
    final_df = pd.merge(middle, d3, on=key, how="outer")
    # final_df.to_csv('peace_final.csv', index=False)
    return final_df


def identify_city(file1, city):
    """
    read letter files, from the content identify city names, create a new
    file with file reference and identified cities.
    :param file1:
    :param city:
    :return: a dataframe with file name and city.
    """
    letter = pd.read_csv(file1)
    city = pd.read_csv(city)
    new_column = []
    city_column = []
    content_column = []
    the_city_column = []
    for row in letter.itertuples():
        new_column.append(row.id)
        content = row.content_v1.lower()
        content_column.append(content)
        found_city = []
        for city_row in city.itertuples():
            pattern = r"\b" + re.escape(city_row.Name.lower()) + r"\b"
            if re.search(pattern, content):
                found = re.search(pattern, content).group()
                found_city.append(found.title())
        city_column.append(" ".join(set(found_city)))
        if len(found_city) > 0:
            the_city_column.append(found_city[0])
        else:
            the_city_column.append("")

    result = pd.DataFrame(columns=["reference", "city", "content"])
    result['reference'] = new_column
    result['content'] = content_column
    result['city'] = city_column
    result['city_link'] = the_city_column
    return result


def process_peace():
    # df = process_file("ocr_text_peace_v3/")
    # print(df.shape)
    # new_df = process_content(df)
    # print(new_df[new_df['content_v2'].isnull()])
    # final = extract_heading(new_df)
    # print(final.info)

    ###work to extract city
    # df = identify_city("peace_final.csv", "export.csv")
    # print(df.head())
    # df.to_csv("city_letter.csv", index=False)

    d1 = pd.read_csv("peace1.csv")
    d2 = pd.read_csv("peace2.csv")
    d3 = pd.read_csv("peace3.csv")
    merged_df = merge_df(d1, d2, d3)
    # merged_df = pd.read_csv("peace_final.csv")
    refs = []
    date_transform = []
    for row in merged_df.itertuples():
        if len(str(row.date)) == 8:
            new_d = datetime.strptime(str(row.date), "%Y%m%d").strftime(
                "%d-%m-%Y")
            date_transform.append(new_d)
        else:
            date_transform.append(None)
        (a, b, c) = row.name.split(".")[1].split("_")
        ref = "_".join([a, b, row.page_id])
        # print(ref)
        refs.append(ref)
    merged_df['date_transform'] = date_transform
    merged_df['reference'] = refs
    merged_df['newpaper'] = "The Namibian"
    merged_df.to_csv("final_peace.csv", index=False)

    # print(merged_df.info())


def process_tax():
    df = process_file("ocr_text_tax_v1/")
    new_df = process_content(df)
    extract_heading(new_df)
    # print(new_df.describe())
    # print(new_df.columns)
    d1 = pd.read_csv("tax1.csv")
    d2 = pd.read_csv("tax2.csv")
    d3 = pd.read_csv("tax3.csv")
    merged_df = merge_df(d1, d2, d3)


def update_peace_letter():
    """
    export the peace letter from Nodegoat with id, match content_v1 with the
    original file, so the updated columns are matched to NodegoatId.
    :return:
    """
    df = pd.read_csv("final_peace.csv")
    # process_peace()
    export = pd.read_csv("letter_export.csv")
    df['content_id'] = df['content_v1'].apply(lambda x: hashlib.md5(
        x.strip().encode('utf-8')).hexdigest()[:20])
    export['content_id'] = df['content_v1'].apply(lambda x: hashlib.md5(
        x.strip().encode('utf-8')).hexdigest()[:20])
    print(export.columns)
    merged = pd.merge(df, export, on="content_id", how="outer")
    merged.to_csv("peace_merged.csv", index=False)


if __name__ == '__main__':
    df = pd.read_csv("peace_merged.csv")
    df['newpaper'] = "The Namibian"
    new_df = df[df['theme'] == "peace"]
    print(new_df.shape)
    new_df.to_csv("peace_merged.csv", index=False)
