"""
This module is needed to transform some names on data downloaded from Twitch and VGChartz in order to make integration
easier. Some games on VGChartz have wrong roman numbers (e.g. Call of Duty Black Ops IIII), hence the need of the
'silly_replace_with_roman' function (which is to be applied to twitch game names).
"""


def silly_replace_with_roman(string):
    """
    Finds arabic numbers in string and replaces them with unary notation
    E.g., replaces 5 with iiiii

    :param string: a string
    :return: string with unary numbers instead of arabic ones
    """
    splitted = string.split()
    for i in range(len(splitted)):
        if splitted[i].isdigit():
            splitted[i] = 'i'*int(splitted[i])
    string = ' '.join(splitted)
    return string


def silly_uniform(string):
    """
    Function that transforms a string by applying the 'silly' replace function,
    deleting the colons and lowering the strings
    :param string: a string
    :return: transformed string
    """
    string = silly_replace_with_roman(string)
    string = string.replace(':', '').lower()
    return string


def arabic_to_roman(num):
    """
    Converts an arabic number to a roman number
    :param num: an integer
    :return: a string
    """
    conv = [[1000, 'M'], [900, 'CM'], [500, 'D'], [400, 'CD'],
        [ 100, 'C'], [ 90, 'XC'], [ 50, 'L'], [ 40, 'XL'],
        [  10, 'X'], [  9, 'IX'], [  5, 'V'], [  4, 'IV'],
        [   1, 'I']]
    roman = ''
    i = 0
    while num > 0:
            while conv[i][0] > num:
                i += 1
            roman += conv[i][1]
            num -= conv[i][0]
    return roman


def replace_with_roman(string):
    """
    Finds arabic numbers in string and replaces them with roman numbers
    E.g., replaces 5 with V

    :param string: a string
    :return: string with roman numbers instead of arabic ones
    """
    splitted = string.split()
    for i in range(len(splitted)):
        if splitted[i].isdigit():
            splitted[i] = arabic_to_roman(int(splitted[i]))
    string = ' '.join(splitted)
    return string


def uniform(string):
    """
    Function that transforms a string by applying the replace_with_roman function,
    deleting the colons and lowering the strings
    :param string: a string
    :return: transformed string
    """
    string = replace_with_roman(string)
    string = string.replace(':', '').lower()
    return string
