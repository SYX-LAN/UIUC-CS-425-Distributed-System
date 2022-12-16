import os
import sys
import time

import torch
import torchvision.transforms as transforms
from PIL import Image
from torch import nn

root = os.path.join(os.path.abspath(os.path.dirname(os.getcwd())), 'main/java/SDFS_files')
y = 0
t = 0
labels = ["cloudy", "haze", "rainy", "shine", "snow", "sunny", "sunrise", "thunder"]


class LeNet5(nn.Module):

    def __init__(self):
        super(LeNet5, self).__init__()

        self.convnet = nn.Sequential(
            nn.Conv2d(1, 6, kernel_size=5, stride=1, padding=0),
            nn.BatchNorm2d(6),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Conv2d(6, 16, kernel_size=5, stride=1, padding=0),
            nn.BatchNorm2d(16),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Conv2d(16, 120, kernel_size=5, stride=1, padding=0),
            nn.BatchNorm2d(120),
            nn.ReLU())

        self.fc = nn.Sequential(
            nn.Linear(120, 84),
            nn.ReLU(),
            nn.Linear(84, 10),
            nn.LogSoftmax(dim=-1))

    def forward(self, x):
        out = self.convnet(x)
        out = out.view(x.size(0), -1)
        out = self.fc(out)
        return out


def predict(imagePath, modelType, stored_name, index, batch):
    predict_file_path = os.path.join(os.path.dirname(os.getcwd()),
                                     'main/java/prediction/' + stored_name)
    global model
    fail_list = []
    if modelType == "weather":
        model = torch.load('./model/weather.pth', map_location=torch.device('cpu'))
        # model = model.to(Common.device)
    elif modelType == "number":
        model = LeNet5()
        # load model
        model.load_state_dict(torch.load('./model/model.ckpt', map_location=torch.device('cpu')))
        model.eval()

    file = open(predict_file_path, 'w')
    if os.path.isfile(imagePath):
        # prediction
        t1 = time.time()
        output = model(transformImg(imagePath, modelType))
        output = torch.argmax(output)
        if modelType == 'number':
            file.write("prediction result: " + str(output.item()))
        else:
            file.write("prediction result: " + labels[output.item()])
        t2 = time.time()
        #print("cost time: " + str(t2 - t1))
    elif os.path.isdir(imagePath):
        files = os.listdir(imagePath)
        start = batch * index
        end = min(start + batch, len(files))
        t1 = time.time()
        for f in files[start:end]:

            output = model(transformImg(os.path.join(imagePath, f), modelType))
            output = torch.argmax(output)
            if modelType == 'number':
                file.write(f + " prediction result: " + str(output.item()) + "\n")
            else:
                file.write(f + " prediction result: " + labels[output.item()] + "\n")
        t2 = time.time()
        #print("cost time: " + str(t2 - t1))
    file.close()


def transformImg(imagePath, modelType) -> torch.Tensor:
    image = Image.open(imagePath)

    if modelType == 'weather':
        transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor()])
    else:
        # resize img
        transform = transforms.Compose([
            transforms.Resize((32, 32)),
            transforms.ToTensor()])
    x = transform(image)
    x = torch.unsqueeze(x, 0)
    return x


if __name__ == '__main__':
    args = sys.argv
    inference_file_name = args[1]
    stored_file_name = args[2]
    modelType = args[3]
    index = args[4]
    batch = args[5]
    file_path = os.path.join(root, inference_file_name)
    predict(file_path, modelType, stored_file_name, int(index), int(batch))
