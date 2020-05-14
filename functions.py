def evaluation(y, y_hat, title = 'Confusion Matrix'):
    '''takes in predicted and true values.
    The function then prints out a classifcation report
    as well as a confusion matrix using seaborn's heatmap.'''
    
    import seaborn as sns
    cm = confusion_matrix(y, y_hat)
    precision = precision_score(y, y_hat, average = 'weighted')
    recall = recall_score(y, y_hat, average = 'weighted')
    accuracy = accuracy_score(y,y_hat)
    print(classification_report(y, y_hat))
    print('Accurancy: ', accuracy)
    sns.heatmap(cm,  cmap= 'Greens', annot=True)
    plt.xlabel('predicted')
    plt.ylabel('actual')
    plt.title(title)
    plt.show()